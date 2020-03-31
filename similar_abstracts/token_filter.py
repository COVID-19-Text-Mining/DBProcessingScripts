# -*- coding: utf-8 -*-

import re

__author__ = "Haoyan Huo"
__maintainer__ = "Haoyan Huo"
__email__ = "haoyan.huo@lbl.gov"


class FilterClass(object):
    _stopwords = {
        'a', 'able', 'about', 'above', 'according', 'accordingly', 'across', 'actually', 'after', 'afterwards',
        'again', 'against', 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'already', 'also', 'although',
        'always', 'am', 'among', 'amongst', 'an', 'and', 'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything',
        'anyway', 'anyways', 'anywhere', 'apart', 'appear', 'appreciate', 'appropriate', 'are', 'around', 'as',
        'aside', 'ask', 'asking', 'associated', 'at', 'available', 'away', 'awfully',

        'b', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind',
        'being', 'believe', 'below', 'beside', 'besides', 'best', 'better', 'between', 'beyond', 'both', 'brief',
        'but', 'by',

        'came', 'can', 'cannot', 'cant', 'cause', 'causes', 'certain', 'certainly', 'changes', 'clearly', 'com',
        'come', 'comes', 'concerning', 'consequently', 'consider', 'considering', 'contain', 'containing', 'contains',
        'corresponding', 'could', 'course', 'currently',

        'definitely', 'described', 'despite', 'did', 'different', 'do', 'does', 'doi', 'doing', 'done', 'down',
        'downwards', 'during',

        'each', 'edu', 'eg', 'either', 'else', 'elsewhere', 'enough', 'entirely', 'especially', 'etc', 'even', 'ever',
        'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except',

        'far', 'few', 'fifth', 'first', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth',
        'from', 'further', 'furthermore',

        'get', 'gets', 'getting', 'given', 'gives', 'go', 'goes', 'going', 'gone', 'got', 'gotten', 'greetings',

        'had', 'happens', 'hardly', 'has', 'have', 'having', 'he', 'hello', 'help', 'hence', 'her', 'here', 'hereafter',
        'hereby', 'herein', 'hereupon', 'hers', 'herself', 'hi', 'him', 'himself', 'his', 'hither', 'hopefully', 'how',
        'howbeit', 'however',

        'i', 'ie', 'if', 'ignored', 'immediate', 'in', 'inasmuch', 'inc', 'indeed', 'indicate', 'indicated',
        'indicates', 'inner', 'insofar', 'instead', 'into', 'inward', 'is', 'it', 'its', 'itself',

        'just',

        'keep', 'keeps', 'kept', 'know', 'knows', 'known',

        'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', 'like', 'liked', 'likely',
        'little', 'license', 'look', 'looking', 'looks',

        'mainly', 'many', 'may', 'maybe', 'me', 'mean', 'meanwhile', 'merely', 'might', 'more', 'moreover', 'most',
        'mostly', 'much', 'must', 'my', 'myself',

        'name', 'namely', 'nd', 'near', 'nearly', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless',
        'new', 'next', 'no', 'nobody', 'non', 'none', 'noone', 'nor', 'normally', 'not', 'nothing', 'novel', 'now',
        'nowhere',

        'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'on', 'once', 'only', 'onto', 'or', 'other',
        'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'own',

        'particular', 'particularly', 'per', 'perhaps', 'placed', 'please', 'plus', 'possible', 'presumably',
        'probably', 'provides',

        'que', 'quite', 'qv',

        'rather', 'rd', 're', 'really', 'reasonably', 'regarding', 'regardless', 'regards', 'relatively',
        'respectively', 'right',

        'said', 'same', 'saw', 'say', 'saying', 'says', 'second', 'secondly', 'see', 'seeing', 'seem', 'seemed',
        'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'several', 'shall',
        'she', 'should', 'since', 'so', 'some', 'somebody', 'somehow', 'someone', 'something', 'sometime', 'sometimes',
        'somewhat', 'somewhere', 'soon', 'sorry', 'specified', 'specify', 'specifying', 'still', 'sub', 'such', 'sup',
        'sure',

        'take', 'taken', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', 'thats', 'the', 'their',
        'theirs', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'therefore', 'therein',
        'theres', 'thereupon', 'these', 'they', 'think', 'third', 'this', 'thorough', 'thoroughly', 'those', 'though',
        'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries',
        'truly', 'try', 'trying',

        'un', 'under', 'unfortunately', 'unless', 'unlikely', 'until', 'unto', 'up', 'upon', 'us', 'use', 'used',
        'useful', 'uses', 'using', 'usually', 'uucp',

        'value', 'various', 'very', 'via', 'viz', 'vs',

        'want', 'wants', 'was', 'way', 'we', 'welcome', 'well', 'went', 'were', 'what', 'whatever', 'when', 'whence',
        'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which',
        'while', 'whither', 'who', 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'willing', 'wish', 'with',
        'within', 'without', 'wonder', 'would', 'would',

        'yes', 'yet', 'you', 'your', 'yours', 'yourself', 'yourselves'
    }
    stopwords = _stopwords | set(x.capitalize() for x in _stopwords)

    SAFE_WORDS = ['h', 'h.', 's', 's.', 'CDEMATERIAL']

    def __init__(self, minimum_number_tokens=10):
        self.word_starting_sentence = re.compile(r'^[A-Z][a-z]*$')
        self.word_re = re.compile(r'^[a-zA-Z][a-zA-Z\-.]*$')
        self.number_re = re.compile(r'^[±+\-]?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?$')
        self.lang_symbols = re.compile(r'^[.~?><:;,(){}[\]\-–_+=!@#$%^&*|\'"]$')
        self.greek_symbols = re.compile(r'^[αβγδεζηθικλμνξοπρσςτυφχψωΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩ]$')
        self.special_symbols = re.compile(r'^°$')
        self.math_symbols = re.compile(r'^[±+\-×/⇌]+$')

        # TODO: complete this list
        number_re_single = r'[±+\-]?\d+(?:\.\d+)?'
        number_re_single_scientific = number_re_single + r'(?:[eE][+\-]?\d+)?'
        units = r'[GMkmμµnpf]?(?:C|F|g|A|mol|l|L|rpm|wt.?|days?|h|hours?|s|min|minutes?|atm|' \
                r'[cd]?m|K|[gG][pP]a|[Ww]eeks?|[Hh]z|bar|eV|Å|%|Torr|psi|V|mmHg)'
        number_and_range = number_re_single_scientific + r'(?:[\-–_~]+' + number_re_single_scientific + r'?)*'
        num_unit_regex = r'^(?P<number>' + number_and_range + r')?' \
                         r'(?P<units>(?:(?:' + units + r')(?:[+\-]?\d+(?:\.\d+)?)?)+)$'
        self.num_unit = re.compile(num_unit_regex)
        self.units = units

        self.minimum_number_tokens = minimum_number_tokens

    def __call__(self, orth, min_length=True):
        if len(orth) == 0:
            return []

        if self.word_starting_sentence.match(orth[0]):
            orth[0] = orth[0].lower()

        new_tokens = []
        for _orth in orth:
            if _orth not in self.stopwords and re.fullmatch(r"[^A-Za-z]+|.|\d+(\.\d+)?|"+self.units, _orth) is None:
                new_tokens.append(_orth)

        if min_length and len(new_tokens) < self.minimum_number_tokens:
            return None

        return new_tokens
