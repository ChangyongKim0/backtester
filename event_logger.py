my_id = 'changyongKim0'

_echo = True
a = 1


def disable():
    global _echo
    write('logger will be disabled.')
    _echo = False


def enable():
    global _echo
    _echo = True
    write('logger is enabled.')


def write(*data):
    if _echo:
        data_string = ''
        for e in data:
            if type(e) != type('aa'):
                data_string += ' ' + str(e)
            else:
                data_string += ' ' + e
        print('({})Log> {}'.format(my_id, data_string))


if __name__ == '__main__':
    disable()
    write('no!!')
    enable()
    write('hello world', '!')
