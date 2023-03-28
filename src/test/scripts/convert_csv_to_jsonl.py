import csv
import json
import os 


def print_help():
    print("Usage:"
          "\n\tpython3 convert_csv_to_json.py <input_filepath>")
    exit()


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] == '--help'):
        print_help()

    input_filename = sys.argv[1]
    output_file_type = "jsonl"
    output_file = os.path.splitext(input_filename)[0] + "." + output_file_type
    
    with open(input_filename, encoding='utf-8') as csvf: 

        csv_reader = csv.DictReader(csvf) 
        with open(output_file, 'w', encoding='utf-8') as jsonf: 
            for row in csv_reader:
                json_string = json.dumps(row)
                jsonf.write(json_string+"\n")

            print("Converted file saved at {}".format(output_file))
            