
def path_to_lines(func):
    def _func(p):
        with open(p, encoding="utf8") as f:
            lines = f.readlines()
        return func(lines)
    return _func

def path_to_text(func):
    def _func(p):
        with open(p, encoding="utf8") as f:
            lines = f.read()
        return func(lines)
    return _func

def iter_strip(lines):
    for line in lines:
        yield line.strip()
