import abc
import argparse


class BaseParser(object):
    """
    Abstract base class for argument parsers
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def group(self):
        """
        Must return dictionary with valid arguments for argparse.add_argument_group()
        Example return:
        {'title': 'Argument group title',
         'description': 'Description for the argument group'}
        """
        pass

    @abc.abstractproperty
    def arguments(self):
        """
        Must return dictionary keyed on the argument name with value being dictionary
        with valid arguments for argparse.add_argument()
        Example return:
        {'my-int-argument': {'help': 'some help', 'default': 1, 'type': int},
         'my-list-argument': {'help': 'some other help', 'nargs': '*'}}
        """
        pass

    def parser(self):
        """
        Returns parser for self only
        """
        return ParserBuilder.build([self])


class ParserBuilder(object):
    """
    Used to build composite parsers from argument group classes
    """

    @staticmethod
    def build(parsers, description=None):
        """
        Assembles a parser using list of {ArgGroup}Args classes
        All of the classes supposed to have 'arguments' and 'group' parameters
        defined
        """
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        for p in parsers:
            p = p()
            group = parser.add_argument_group(**p.group)
            for name, kwargs in p.arguments.items():
                group.add_argument('--{}'.format(name), **kwargs)
        return parser

    @staticmethod
    def log_args(args, parsers, logger):
        """
        Logs arguments and values provided by user
        """
        for parser in parsers:
            args_to_print = [
                arg for arg in args._get_kwargs() if arg[0].replace('_', '-') in parser().arguments
            ]
            logger.info("\t{}:".format(parser.__name__))
            for name, value in args_to_print:
                if any([k in name.lower() for k in ['pass', 'key', 'secret']]):
                    logger.info('{}={}'.format(name, 'VALUE_IS_SECRET'))
                else:
                    logger.info('{}={}'.format(name, value))

    @staticmethod
    def get_arg_attrnames(parsers):
        """
        Returns attribute names corresponding to arguments of :parsers
        """
        attrs = []
        for p in parsers:
            for arg in p().arguments:
                attrs.append(arg.replace('-', '_'))
        return attrs

    @classmethod
    def get_environment_dict(cls, args, parsers):
        """
        Given list of args and parsers, return environment variable dictionary
        e.g.  args = Namespace(my_int=1, my_list=[1, 2, 3]) will return:
        {'MY_INT': '1', 'MY_LIST': '1,2,3'}
        """
        env_dict = {}
        for parser in parsers:
            for argname in parser().arguments:
                argname = argname.replace('-', '_')
                value = getattr(args, argname)
                if isinstance(value, list):
                    if value == []:
                        continue
                    value = ','.join(map(str, value))
                else:
                    value = str(value)
                env_dict[argname.upper()] = value
        return env_dict
