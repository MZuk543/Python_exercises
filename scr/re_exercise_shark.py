# Two command-line arguments needed to run the script: input and output file name or path\name
import re
import sys


def main():
    # command-line arguments: input and output file name
    if len(sys.argv) == 1:
        sys.exit("Missing command-line arguments")
    elif len(sys.argv) != 3:
        sys.exit("Not valid command-line arguments")
    else:
        cmd_input_f = sys.argv[1]
        cmd_output_f = sys.argv[2]
        # Open the file to parse:
        lines_output = []
        try:
            with open(cmd_input_f, "r") as f_input:
                for line in f_input:
                    lines_output.append(re_output(line))
                with open(cmd_output_f, "w") as f_output:
                    for idx, line in enumerate(lines_output):
                        if len(line) > 0:
                            f_output.write(str(line))
                            if idx < len(lines_output) - 1:
                                f_output.write("\n")
        except IOError as e:
            sys.exit(e)
        #
        # here you can add more exceptions before finally
        #
        finally:  # will be executed no matter what error could occur
            print("Done!")


def re_output(text=None):
    if text is None:
        return ""
    else:
        # find USERS names:
        re_un = re.compile(r"[a-zA-Z]{2}[0-9]+(?=[,\n\r]|$)", re.UNICODE)
        # if there are USERS names
        if re_un.search(text):
            result = str()
            # find numbers from NAME
            re_nn = re.compile(r"(?<=[-_])\d+(?=[_ ])", re.UNICODE)
            nn = re_nn.search(text)
            if nn:
                result += nn.group()
            result += ","
            # find server name from NAME
            re_sn = re.compile(r"[a-zA-Z0-9-_]+_+[a-zA-Z0-9-_]+(?=[ \n\r])", re.UNICODE)
            sn = re_sn.search(text)
            result += sn.group()
            result += ","
            # find unique users names from USERS
            un = re_un.findall(text)
            for u in un:
                result += u
                result += ";"
            # remove the last ";"
            return result[0:len(result)-1]
        else:
            # no unique user names in USERS:
            return ""


if __name__ == '__main__':
    main()
