COLORS = {
    'red':    '\033[91m',
    'blue':   '\033[94m',
    'green':  '\033[32m',
    'black':  '\033[30m',
    'yellow': '\033[33m',
    'bold':   '\033[1m',
}

CLEAR = '\033[0m'

globals_map = globals()
for color, code in COLORS.items():
    globals_map[color] = (lambda x, code=code: code + x + CLEAR)

__all__ = COLORS.keys()
