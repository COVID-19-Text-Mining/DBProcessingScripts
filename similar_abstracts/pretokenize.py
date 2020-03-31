from token_filter import FilterClass
from preprocessing import TextPreprocessor
import re


class PreTokenize:
    filter = FilterClass()

    @classmethod
    def tokenize_one(cls, text, min_length):
        processed_text = TextPreprocessor(text)
        return cls.filter(
            processed_text.get_words(),
            min_length
        )

    @classmethod
    def tokenize(cls, raw_text, min_length, keep_para=False):
        paras = re.split(r"\r\n", raw_text)
        tokens = []
        for para in paras:
            token = PreTokenize.tokenize_one(para, min_length)
            if token:
                tokens.extend(token)
                if keep_para:
                    tokens.append("\n")
        return tokens
