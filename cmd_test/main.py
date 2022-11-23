import cmd2

class Cli(cmd2.Cmd):
    def __init__(self):
        super().__init__()
        self.prompt = "brew -->"


if __name__ == "__main__":
    cli = Cli()
