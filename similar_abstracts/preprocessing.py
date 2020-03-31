import logging
import re

import spacy

__all__ = ['TextPreprocessor']

__author__ = "Haoyan Huo"
__maintainer__ = "Haoyan Huo"
__email__ = "haoyan.huo@lbl.gov"

_nlp = None


class TextPreprocessor(object):
    VERSION = '0.1.0'

    def __init__(self, text):
        """
        Create a new TextPreprocessor instance from a paragraph.

        :param text: the paragraph to be processed.
        :type text: str
        """
        self.doc = self._process(text)

    def _process(self, text):
        doc = self._make_doc(text)

        return doc

    def _make_doc(self, text):
        global _nlp
        if _nlp is None:
            _nlp = spacy.load('en_core_web_sm', disable=['tagger', 'parser', 'ner'])
            _nlp.max_length = 1_000_000
            # _nlp.make_doc = ChemDataInfoTokenizer(_nlp)
            logging.info('Loading SpaCy model, with custom tokenizer.')
            for name, obj in _nlp.pipeline:
                logging.info('SpaCy model has pipeline %s: %r', name, obj)

        text = re.sub(r"https?\S+", "", text)  # clear all the urls
        return _nlp(text)

    def get_words(self):
        """
        Get all words in the paragraph.

        :return: a list of lists of words.
        :rtype: list
        """
        return [x.orth_ for x in self.doc]
