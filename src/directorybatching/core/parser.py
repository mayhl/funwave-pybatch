


class Invertable:

    def __init__(self, forward, reverse):
        self._forward = forward
        self._reverse = reverse

    def forward(self, *args): return self._forward(*args)
    def reverse(self, *args): return self._reverse(*args)

    def test(self): pass


class NameValidator(Invertable):

    def __init__(self, forward, reverse, is_skip_check=False):
        self._is_skip_check = is_skip_check
        super().__init__(forward, reverse)


    # Standardizing output to allow optional 
    # return string in _forward, e.g.,
    #     True | None | (True, 'eg1') 
    def forward(self, name, string):
        rtn_val = self._forward(name, string)
        if not type(rtn_val) is tuple: rtn_val = (rtn_val, string)
        return rtn_val
        
    def test(self, name, string, is_allow_none=True):

        string = self._forward(name, string)

        if string is None:
            return is_allow_none, "Name parser returned None, directory will be ignored."

        rname = self._reverse(name, rtnvals)

        if not name == rname: 
            return False, "Names do not match. Input: %s | Output: %s" % (name, rname)

        return True, None


class StartsWith(NameValidator):

    def __init__(self):
        super().__init__(self.__forward, StartsWith.__reverse)

    #@classmethod
    def __forward(self, name, string):
        is_start = string.startswith(name) 
        if is_start: string = string[len(name):]
        return is_start, string

    @classmethod
    def __reverse(cls, name, end_string):
        return "%s%s" % (name, end_string)

class MatchAt(NameValidator):

    def __init__(self, index, delimiter='_'):

        self._del = delimiter

        if index == 0 or index == 'first':
           super().__init__(self.__first_forward, self.__first_reverse)
        elif index == 'last':
           super().__init__(self.__last_forward, self.__last_reverse)
        else:
            raise NotImplementedError()

    @classmethod
    def Last(cls, delimiter='_'):
        return cls('last', delimiter)
    @classmethod
    def First(cls, delimiter='_'):
        return cls('first', delimiter)

    def __first_forward(self, name, string):
        first, *last = string.split(self._del)
        last = self._del.join(last)
        is_valid = first == name
        return is_valid, last

    def __first_reverse(self, name, string):
            return name + self._del + string

    def __last_forward(self, name, string):
        *begin, last = string.split(self._del) 
        begin = self._del.join(begin)
        is_valid = last == name
        return is_valid, begin

    def __last_reverse(self, name, string):
        return string + self._del + name

class SplitAt(NameValidator):

    def __init__(self, index, delimiter='_'):

        self._del = delimiter

        if index == 0 or index == 'first':
           super().__init__(self.__first_forward, self.__first_reverse)
        elif index == 'last':
           super().__init__(self.__last_forward, self.__last_reverse)
        else:
            raise NotImplementedError()

    @classmethod
    def Last(cls, delimiter='_'):
        return cls('last', delimiter)
    @classmethod
    def First(cls, delimiter='_'):
        return cls('first', delimiter)

    def __first_forward(self, name, string):
        first, *last = string.split(self._del)
        last = self._del.join(last)
        is_valid = first == name
        return True, (first, last)

    def __first_reverse(self, name, string):
            return name + self._del + string

    def __last_forward(self, name, string):
        *first, last = string.split(self._del) 
        first = self._del.join(first)
        is_valid = last == name
        return True, (first, last)

    def __last_reverse(self, name, string):
        return string + self._del + name


class ValueParser(Invertable):

    def __init__(self, forward, reverse, dtype, is_simple=True):

        if is_simple:
            fp = lambda n, x: {n : forward(x)}
            rp = lambda n, x: reverse(x[n])
        else:
            fp, rp = forward, reverse

        super().__init__(fp, rp)
        self._dtype = dtype

    @property
    def dtype(self): return self._dtype

    @classmethod
    def get_dummy(cls, dtype=str):
        return ValueParser(lambda x: x, lambda x: x, dtype)


class ValidatorParser(Invertable):

    def __init__(self, validator, parser, preprocessor=None):
        self._validator = validator
        self._parser = parser
        self._preproc = preprocessor
        self._has_preproc = not preprocessor is None

    def raw_forward(self, value):
        k = 'dummy'
        return self._parser.forward(k ,value)[k]

    def raw_reverse(self, value):
        k = 'dummy'
        test = self._parser.reverse(k, {k: value})
        return test

    @property 
    def dtype(self): return self._parser.dtype

    @property
    def has_preprocessor(self): return self._has_preproc

    def forward(self, name, string):

        if self._has_preproc:
            string, rtnvval = self._preproc.forward(name, string)

        is_valid, string = self._validator.forward(name, string)
        if not is_valid: return None

        rtnval = self._parser.forward(name, string)
        if self._has_preproc: 
            rtnval['virtual'] = rtnvval

        return rtnval

    def reverse(self, name, rtnval, ignore_preproc=False):


        if self._has_preproc and not ignore_preproc:
            rtnvvals = rtnval['virtual']
            #del rtnval['virtual']

        string = self._parser.reverse(name, rtnval)
        string = self._validator.reverse(name, string)

        if self._has_preproc and not ignore_preproc:
            string = self._preproc.reverse(string, rtnvval)

        return string

    @classmethod
    def get_startswith(cls, *args, **kwargs):
        return cls(StartsWith(), *args, **kwargs)

class Preprocessor(ValidatorParser):

    def __init__(self, validator, parser, name):
        super().__init__(validator, parser)
        self._name = name
    
    @property
    def name(self): return self._name

    def forward(self, name,  string):
        _, (string, value) = self._validator.forward(name, string)
        return string, self._parser.forward(self._name, value)
    
    def reverse(self, string, rtnvals):
        vstring = self._parser.reverse(self._name, rtnvals)
        return self._validator.reverse(stirng, vstring)

    @classmethod
    def SplitAtLast(cls, name, parser=None, delimiter='_'):
        validator = SplitAt.Last(delimiter)
        if parser is None: parser = ValueParser.get_dummy()
        return cls(validator, parser, name)

    @classmethod
    def JobID(cls, delimiter='_'): 
        return cls.SplitAtLast('jobid', delimiter=delimiter)


class Integer(ValidatorParser):

    def __init__(self, validator, n=0, is_flip_sign=False, preprocessor=None):
        s = -1 if is_flip_sign else 1
        fmt = "%d" if n==0 else "%%0%dd" % n
        parser = ValueParser(int, lambda x: fmt % (s*x), int)
        super().__init__(validator, parser, preprocessor=preprocessor)
    
class Decimal(ValidatorParser):

    def __init__(self, validator, digits, offset=0, is_flip_sign=False, preprocessor=None):
        s = -1 if is_flip_sign else 1

        if type(digits) is str:
            raise NotImplementedError()
        else:
            pow = digits + offset - 1
            fmt = '%%0%dd' % digits
            forward = lambda x: float(s*int(x)*10**-pow)
            reverse = lambda x: fmt % (s*x*10**pow)
            parser = ValueParser(forward, reverse, float)
 
        super().__init__(validator, parser, preprocessor=preprocessor)


