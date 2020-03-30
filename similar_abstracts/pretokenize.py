from token_filter import FilterClass
from preprocessing import TextPreprocessor
import re


class PreTokenize:
    filter = FilterClass()

    @classmethod
    def tokenize_one(cls, text, min_length):
        processed_text = TextPreprocessor(text)
        return cls.filter(
            processed_text.get_words(lemma=False),
            processed_text.get_words(lemma=True),
            processed_text.get_pos(),
            min_length
        )

    @classmethod
    def tokenize(cls, raw_text, min_length):
        paras = re.split(r"\r\n", raw_text)
        tokens = []
        for para in paras:
            token = PreTokenize.tokenize_one(para, min_length)
            if token:
                tokens.extend(token)
        return tokens
