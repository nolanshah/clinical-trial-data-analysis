import argparse
import glob
import os
import xml.etree.cElementTree as ElemTree
from collections import OrderedDict

import orjson
import xmlschema
from xmlschema.exceptions import XMLSchemaValueError


class ParquetFriendlyConverter(xmlschema.XMLSchemaConverter):
    """
    XML schema based converter class for PARQUET-friendly JSON.
    This is ripped almost directly from https://github.com/blackrock/xml_to_parquet/
    """

    def __init__(self, namespaces=None, dict_class=None, list_class=None, **kwargs):
        kwargs.update(attr_prefix="", text_key=None, cdata_prefix=None)
        super(ParquetFriendlyConverter, self).__init__(
            namespaces, dict_class or OrderedDict, list_class, **kwargs
        )

    def __setattr__(self, name, value):
        if name in ("text_key", "cdata_prefix") and value is not None:
            raise XMLSchemaValueError(f"Wrong value {value} for the attribute {name} of a {type(self)}.")
        super(xmlschema.XMLSchemaConverter, self).__setattr__(name, value)

    @property
    def lossless(self):
        return False

    def element_decode(self, data, xsd_element, xsd_type=None, level=0):
        if data.attributes:
            self.attr_prefix = xsd_element.local_name + "___" # use `___` instead of `@` b/c of AWS Athena
            result_dict = self.dict(
                [(k, v) for k, v in self.map_attributes(data.attributes)]
            )
        else:
            result_dict = self.dict()

        if xsd_element.type.is_simple() or xsd_element.type.has_simple_content():
            result_dict[xsd_element.local_name] = (
                data.text if data.text is not None and data.text != "" else None
            )

        if data.content:
            for name, value, xsd_child in self.map_content(data.content):
                if value:
                    if xsd_child.local_name:
                        name = xsd_child.local_name
                    else:
                        name = name[2 + len(xsd_child.namespace):]

                    if xsd_child.is_single():
                        if hasattr(xsd_child, "type") and (
                                xsd_child.type.is_simple()
                                or xsd_child.type.has_simple_content()
                        ):
                            for k in value:
                                result_dict[k] = value[k]
                        else:
                            result_dict[name] = value
                    else:
                        if (
                                xsd_child.type.is_simple()
                                or xsd_child.type.has_simple_content()
                        ) and not xsd_child.attributes:
                            try:
                                result_dict[name].append(list(value.values())[0])
                            except KeyError:
                                result_dict[name] = self.list(value.values())
                            except AttributeError:
                                result_dict[name] = self.list(value.values())
                        else:
                            try:
                                result_dict[name].append(value)
                            except KeyError:
                                result_dict[name] = self.list([value])
                            except AttributeError:
                                result_dict[name] = self.list([value])
        if level == 0:
            return self.dict([(xsd_element.local_name, result_dict)])
        else:
            return result_dict


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--src', type=str, required=True,
                        metavar='./source/',
                        help='source path')
    parser.add_argument('--dst', type=str, required=True,
                        metavar='./destination/',
                        help='destination path')
    parser.add_argument('--schema', type=str, required=True,
                        metavar='./schema/public.xsd',
                        help='schema file path')
    parser.add_argument('--chunk-size', type=int, default=1000,
                        metavar='1000',
                        help='number of records per output file')
    args = parser.parse_args()

    print(args)

    # All the conversion magic happens in the converter. AWS Athena and the PARQUET format generally
    # have some constraints on character types and type composition this converter takes care of.
    schema = xmlschema.XMLSchema(args.schema, converter=ParquetFriendlyConverter)

    # the zip contains a structure like `/AllPublicXML/NCT0001xxxx/NCT00010001.xml`
    # {args.src} is expected to be `/AllPublicXML`; from which we traverse down to get the xml files
    all_src_file_paths = [x for x in sorted(glob.glob(os.path.join(args.src, 'NCT*/', '*.xml')))]

    # split the list of source files into chunks of size {args.chunk_size}
    src_file_path_chunks = [all_src_file_paths[i:i + args.chunk_size]
                            for i in range(0, len(all_src_file_paths), args.chunk_size)]

    for chunk_index, chunk_data in enumerate(src_file_path_chunks):
        print(f"Processing chunk {chunk_index}")
        dst_file_path = os.path.join(args.dst, f"data-{chunk_index}.json")  # "{dst_dir}/{dst_file_name}"

        with open(dst_file_path, 'ab') as dst_file:
            for src_file_path in chunk_data:
                with open(src_file_path, 'rb') as src_file:
                    data_obj = ElemTree.parse(src_file)
                    data_dict = schema.to_dict(data_obj, process_namespaces=False)
                    data_str = orjson.dumps(data_dict)
                dst_file.write(data_str)
                dst_file.write(b"\n")


if __name__ == '__main__':
    main()
