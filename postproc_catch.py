import argparse
import subprocess
import glob

PROC_ENV_KEY = None
PROC_ARG_KEY = "CATCHARG"
PROC_INP_KEY = "CATCHINP"
PROC_NAME = "CATCH"


def run(argstr, inputs, env):
    print(argstr)
    parser = argparse.ArgumentParser(description="Log certain info")
    parser.add_argument(
        "destination", type=str, help="The directory to which the RAW files are copied"
    )
    parser.add_argument(
        "-t",
        nargs="*",
        type=float,
        help="The durations of each post-processing step (use $time$)",
    )
    parser.add_argument(
        "-p",
        nargs="*",
        type=str,
        help="The names of each post-processing step (use $proc$)",
    )
    parser.add_argument(
        "--threshold",
        default=15,
        type=float,
        help="The threshold underneath which a rawspec's duration is taken to be indicative of an erroneous raw file set. [15]",
    )

    args = parser.parse_args(argstr.split(" "))

    if len(inputs) != 1:
        print(
            "The Exceptions stage will would copy the raw files to arg.d if the rawspec run took less than %0.4f seconds."
            % args.threshold
        )

    for procI, proc in enumerate(args.p):
        if proc.lower() == "rawspec":
            if args.t[procI] < args.threshold:
                destination = args.destination

                print(
                    "Rawspec run took %0.4fs < %0.4fs" % (args.t[procI], args.threshold)
                )
                print("\tcopying RAW files to %s" % (destination))

                cmd = ["mkdir", "-p", destination]
                print(" ".join(cmd))
                subprocess.run(cmd)
                print()

                matchedfiles = glob.glob(inputs[0] + "*.raw")
                for mfile in matchedfiles:
                    cmd = ["cp", mfile, destination]
                    print(" ".join(cmd))
                    subprocess.run(cmd)  # , env=env)
                print()
            break

    return []


if __name__ == "__main__":
    run(
        "err-seti-node4.0/guppi_59229_47368_006379_Unknown_0001/ -t 0.011775016784667969 -p RAWSPEC",
        ["sadf"],
        None,
    )
