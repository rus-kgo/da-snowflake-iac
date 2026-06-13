"""SQLIaC entry point."""

from sqliac import cli, __prog_name__

if __name__ == "__main__":
    cli.main(prog_name=__prog_name__)
