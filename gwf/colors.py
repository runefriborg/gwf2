COLORS = {
    'red':    '\033[91m',
    'blue':   '\033[94m',
    'green':  '\033[32m',
    'black':  '\033[30m',
    'yellow': '\033[33m',
    'bold':   '\033[1m',
}

CLEAR = '\033[0m'

ACTIVATED = True

globals_map = globals()
for color, code in COLORS.items():
    globals_map[color] = (lambda x, code=code: code + x + CLEAR
                          if ACTIVATED else x)


def off():
    globals()['ACTIVATED'] = False


def on():
    globals()['ACTIVATED'] = True

__all__ = COLORS.keys() + ['on', 'off']
