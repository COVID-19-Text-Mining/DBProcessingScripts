import json
import numpy as np
from common_utils import get_mongo_db
from bson import json_util
import summa
import yake
import pke
import regex
import string
from nltk.corpus import stopwords

class KeywordsExtractorBase():
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', 'Base')
        # output_format could be words_only or words_and_scores
        self.output_format = kwargs.get('output_format', 'words_only')
        self.score_threshold = kwargs.get('score_threshold', None)

    def process(self, text):
        words_and_scores = self._process(text)
        if self.score_threshold:
            words_and_scores = list(filter(
                lambda x: x[1] > self.score_threshold,
                words_and_scores
            ))
        print('{} words_and_scores'.format(self.name))
        print(words_and_scores)
        print()
        if self.output_format == 'words_and_scores':
            return words_and_scores
        elif self.output_format == 'words_only':
            return list(set([item[0] for item in words_and_scores]))
        else:
            raise AttributeError(
                'output_format {} not recognized'.format(self.output_format)
            )

    def _process(self, text):
        raise NotImplementedError

    def hightlight_keywords(self,
                            text,
                            keywords,
                            light_color='#ffea593d',
                            deep_color='#ffc107'):
        hightlighted_html = ''
        all_hightlights = []
        tokens = self.full_tokenize(text)
        for t in tokens:
            t['background_color'] = []

        for w in keywords:
            matches = regex.finditer(r'\b{}\b'.format(w), text, flags=regex.IGNORECASE)
            all_hightlights.extend([
                {
                    'start': m.start(),
                    'end': m.end(),
                    'text': m.group(),
                }
                for m in matches
            ])

        all_hightlights = sorted(all_hightlights, key=lambda x: x['start'])

        for h in all_hightlights:
            for t in tokens:
                if (t['start'] >= h['start'] and t['end'] <= h['end']):
                    t['background_color'].append(light_color)

        for t in tokens:
            color_len = len(t['background_color'])
            if color_len == 0:
                hightlighted_html += t['text']
            elif color_len == 1:
                hightlighted_html += \
                    '<span style="background-color:{background_color};">{text}</span>'.format(
                    background_color=t['background_color'][0],
                    text=t['text']
                )
            else:
                hightlighted_html += \
                    '<span style="background-color:{background_color};">{text}</span>'.format(
                    background_color=deep_color,
                    text=t['text']
                )
        return hightlighted_html

    def full_tokenize(self, text):
        tokens = []
        for m in regex.finditer(r'\p{Punct}|.+?\b', text):
            tokens.append({
                'start': m.start(),
                'end': m.end(),
                'text': m.group(),
            })
        return tokens

    def clean_html_tag(self, text):
        new_text = regex.sub(r'<.{1,10}?>', '', text)
        return new_text

class KeywordsExtractorSumma(KeywordsExtractorBase):
    def __init__(self, split=False, scores=False, **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'Summa')
        self.split=split
        self.scores = scores

    def _process(self, text):
        keywords = summa.keywords.keywords(text, split=self.split, scores=self.scores)
        return keywords


class KeywordsExtractorYake(KeywordsExtractorBase):
    def __init__(self, max_ngram_size=3, window_size=1, **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'Yake')
        self.max_ngram_size = max_ngram_size
        self.window_size = window_size
        self.kw_extractor = yake.KeywordExtractor(
            n=self.max_ngram_size,
            windowsSize=self.window_size,
        )

    def _process(self, text):
        keywords = self.kw_extractor.extract_keywords(text)
        return keywords

class KeywordsExtractorTfIdf(KeywordsExtractorBase):
    def __init__(self, max_ngram_size=3, **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'TfIdf')
        self.max_ngram_size = max_ngram_size
        self.pos = {'NOUN', 'PROPN', 'ADJ'}
        self.stoplist = list(string.punctuation)
        self.stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
        self.stoplist += stopwords.words('english')
        self.kw_extractor = pke.unsupervised.TfIdf()

    def _process(self, text):
        # load the content of the document.
        self.kw_extractor.load_document(
            input=text,
            language='en',
        )

        # select the longest sequences of nouns and adjectives, that do
        #    not contain punctuation marks or stopwords as candidates.
        self.kw_extractor.candidate_selection(
            n=self.max_ngram_size,
            stoplist=self.stoplist,
        )

        # build topics by grouping candidates with HAC (average linkage,
        #    threshold of 1/4 of shared stems). Weight the topics using random
        #    walk, and select the first occuring candidate from each topic.
        self.kw_extractor.candidate_weighting()

        # get the 10-highest scored candidates as keyphrases
        num_keywords = len(self.kw_extractor.candidates)
        if num_keywords > 10:
            keywords = self.kw_extractor.get_n_best(n=10)
        else:
            keywords = self.kw_extractor.get_n_best(n=num_keywords)

        return keywords

class KeywordsExtractorKPMiner(KeywordsExtractorBase):
    def __init__(self, lasf=3, cutoff=200, alpha=2.3, sigma=3.0, **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'KPMiner')
        self.lasf = lasf
        self.cutoff = cutoff
        self.alpha = alpha
        self.sigma = sigma
        self.pos = {'NOUN', 'PROPN', 'ADJ'}
        self.stoplist = list(string.punctuation)
        self.stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
        self.stoplist += stopwords.words('english')
        self.kw_extractor = pke.unsupervised.KPMiner()

    def _process(self, text):
        # load the content of the document.
        self.kw_extractor.load_document(
            input=text,
            language='en',
        )

        # select the longest sequences of nouns and adjectives, that do
        #    not contain punctuation marks or stopwords as candidates.
        self.kw_extractor.candidate_selection(
            lasf=self.lasf,
            cutoff=self.cutoff,
            stoplist=self.stoplist,
        )

        # build topics by grouping candidates with HAC (average linkage,
        #    threshold of 1/4 of shared stems). Weight the topics using random
        #    walk, and select the first occuring candidate from each topic.
        self.kw_extractor.candidate_weighting(
            alpha=self.alpha,
            sigma=self.sigma,
        )

        # get the 10-highest scored candidates as keyphrases
        num_keywords = len(self.kw_extractor.candidates)
        if num_keywords > 10:
            keywords = self.kw_extractor.get_n_best(n=10)
        else:
            keywords = self.kw_extractor.get_n_best(n=num_keywords)

        return keywords


class KeywordsExtractorSingleRank(KeywordsExtractorBase):
    def __init__(self, window=10, **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'SingleRank')
        self.window = window
        self.pos = {'NOUN', 'PROPN', 'ADJ'}
        self.stoplist = list(string.punctuation)
        self.stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
        self.stoplist += stopwords.words('english')
        self.kw_extractor = pke.unsupervised.SingleRank()

    def _process(self, text):
        # load the content of the document.
        self.kw_extractor.load_document(
            input=text,
            language='en',
        )

        # select the longest sequences of nouns and adjectives, that do
        #    not contain punctuation marks or stopwords as candidates.
        self.kw_extractor.candidate_selection(
            pos=self.pos,
        )

        # build topics by grouping candidates with HAC (average linkage,
        #    threshold of 1/4 of shared stems). Weight the topics using random
        #    walk, and select the first occuring candidate from each topic.
        self.kw_extractor.candidate_weighting(
            window=self.window,
            pos=self.pos,
        )

        # get the 10-highest scored candidates as keyphrases
        num_keywords = len(self.kw_extractor.candidates)
        if num_keywords > 10:
            keywords = self.kw_extractor.get_n_best(n=10)
        else:
            keywords = self.kw_extractor.get_n_best(n=num_keywords)

        return keywords

class KeywordsExtractorTopicalPageRank(KeywordsExtractorBase):
    def __init__(self, window=10, lda_model=None, **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'TopicalPageRank')
        self.window = window
        self.lda_model = lda_model
        self.pos = {'NOUN', 'PROPN', 'ADJ'}
        self.stoplist = list(string.punctuation)
        self.stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
        self.stoplist += stopwords.words('english')
        # define the grammar for selecting the keyphrase candidates
        self.grammar = "NP: {<ADJ>*<NOUN|PROPN>+}"
        self.kw_extractor = pke.unsupervised.TopicalPageRank()

    def _process(self, text):
        # load the content of the document.
        self.kw_extractor.load_document(
            input=text,
            language='en',
        )

        # select the longest sequences of nouns and adjectives, that do
        #    not contain punctuation marks or stopwords as candidates.
        self.kw_extractor.candidate_selection(
            grammar=self.grammar,
        )
        num_keywords = len(self.kw_extractor.candidates)


        # build topics by grouping candidates with HAC (average linkage,
        #    threshold of 1/4 of shared stems). Weight the topics using random
        #    walk, and select the first occuring candidate from each topic.
        if num_keywords > 0:
            # try:
            self.kw_extractor.candidate_weighting(
                window=self.window,
                pos=self.pos,
                lda_model=self.lda_model,
            )
            # except:
            #     num_keywords = 0

        # get the 10-highest scored candidates as keyphrases
        if num_keywords > 10:
            keywords = self.kw_extractor.get_n_best(n=10)
        elif num_keywords > 0:
            keywords = self.kw_extractor.get_n_best(n=num_keywords)
        else:
            keywords = []

        return keywords

class KeywordsExtractorPositionRank(KeywordsExtractorBase):
    def __init__(self, window=10, max_ngram_size=3, **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'PositionRank')
        self.window = window
        self.max_ngram_size = max_ngram_size
        self.pos = {'NOUN', 'PROPN', 'ADJ'}
        self.stoplist = list(string.punctuation)
        self.stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
        self.stoplist += stopwords.words('english')
        # define the grammar for selecting the keyphrase candidates
        self.grammar = "NP: {<ADJ>*<NOUN|PROPN>+}"
        self.kw_extractor = pke.unsupervised.PositionRank()

    def _process(self, text):
        # load the content of the document.
        self.kw_extractor.load_document(
            input=text,
            language='en',
        )

        # select the longest sequences of nouns and adjectives, that do
        #    not contain punctuation marks or stopwords as candidates.
        self.kw_extractor.candidate_selection(
            grammar=self.grammar,
            maximum_word_number=self.max_ngram_size,
        )
        num_keywords = len(self.kw_extractor.candidates)


        # build topics by grouping candidates with HAC (average linkage,
        #    threshold of 1/4 of shared stems). Weight the topics using random
        #    walk, and select the first occuring candidate from each topic.
        if num_keywords > 0:
            # try:
            self.kw_extractor.candidate_weighting(
                window=self.window,
                pos=self.pos,
            )
            # except:
            #     num_keywords = 0

        # get the 10-highest scored candidates as keyphrases
        if num_keywords > 10:
            keywords = self.kw_extractor.get_n_best(n=10)
        elif num_keywords > 0:
            keywords = self.kw_extractor.get_n_best(n=num_keywords)
        else:
            keywords = []

        return keywords

class KeywordsExtractorMultipartiteRank(KeywordsExtractorBase):
    def __init__(self, alpha=1.1, threshold=0.74, method='average', **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'MultipartiteRank')
        self.alpha = alpha
        self.threshold = threshold
        self.method = method
        self.pos = {'NOUN', 'PROPN', 'ADJ'}
        self.stoplist = list(string.punctuation)
        self.stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
        self.stoplist += stopwords.words('english')
        # define the grammar for selecting the keyphrase candidates
        self.grammar = "NP: {<ADJ>*<NOUN|PROPN>+}"
        self.kw_extractor = pke.unsupervised.MultipartiteRank()

    def _process(self, text):
        # load the content of the document.
        self.kw_extractor.load_document(
            input=text,
            language='en',
        )

        # select the longest sequences of nouns and adjectives, that do
        #    not contain punctuation marks or stopwords as candidates.
        self.kw_extractor.candidate_selection(
            pos=self.pos,
            stoplist=self.stoplist,
        )
        num_keywords = len(self.kw_extractor.candidates)


        # build topics by grouping candidates with HAC (average linkage,
        #    threshold of 1/4 of shared stems). Weight the topics using random
        #    walk, and select the first occuring candidate from each topic.
        if num_keywords > 0:
            try:
                self.kw_extractor.candidate_weighting(
                    alpha=self.alpha,
                    threshold=self.threshold,
                    method=self.method,
                )
            except:
                num_keywords = 0

        # get the 10-highest scored candidates as keyphrases
        if num_keywords > 10:
            keywords = self.kw_extractor.get_n_best(n=10)
        elif num_keywords > 0:
            keywords = self.kw_extractor.get_n_best(n=num_keywords)
        else:
            keywords = []

        return keywords


class KeywordsExtractorTopicRank(KeywordsExtractorBase):
    def __init__(self, threshold=0.74, method='average', heuristic=None, **kwargs):
        super().__init__(**kwargs)
        self.name = kwargs.get('name', 'TopicRank')
        self.threshold = threshold
        self.method = method
        self.heuristic = heuristic
        self.pos = {'NOUN', 'PROPN', 'ADJ'}
        self.stoplist = list(string.punctuation)
        self.stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
        self.stoplist += stopwords.words('english')
        self.kw_extractor = pke.unsupervised.TopicRank()

    def _process(self, text):
        # load the content of the document.
        self.kw_extractor.load_document(
            input=text,
            language='en',
        )

        # select the longest sequences of nouns and adjectives, that do
        #    not contain punctuation marks or stopwords as candidates.
        self.kw_extractor.candidate_selection(
            pos=self.pos,
            stoplist=self.stoplist,
        )
        num_keywords = len(self.kw_extractor.candidates)


        # build topics by grouping candidates with HAC (average linkage,
        #    threshold of 1/4 of shared stems). Weight the topics using random
        #    walk, and select the first occuring candidate from each topic.
        if num_keywords > 0:
            try:
                self.kw_extractor.candidate_weighting(
                    threshold=self.threshold,
                    method=self.method,
                    heuristic=self.heuristic,
                )
            except:
                num_keywords = 0

        # get the 10-highest scored candidates as keyphrases
        if num_keywords > 10:
            keywords = self.kw_extractor.get_n_best(n=10)
        elif num_keywords > 0:
            keywords = self.kw_extractor.get_n_best(n=num_keywords)
        else:
            keywords = []

        return keywords


def phrase_match(ref_phrase, pred_phrase):
    ref_words = set(ref_phrase.split())
    pred_words = set(pred_phrase.split())
    matched_words = ref_words & pred_words
    ratio_match = len(matched_words) / max(len(ref_words), 1)
    return ratio_match


def evaluation_a_doc(ref_words, pred_words, ignore_case=True):
    num_ref = len(ref_words)
    num_pred = len(pred_words)
    num_positive = 0

    if ignore_case:
        ref_words = [w.lower() for w in ref_words]
        pred_words = [w.lower() for w in pred_words]

    for r_w in ref_words:
        for p_w in pred_words:
            num_positive += phrase_match(r_w, p_w)

    precision = num_positive / max(num_pred, 1.0)
    recall = num_positive / max(num_ref, 1.0)
    F1 = 2*precision*recall/max((precision+recall), 1E-6)
    return precision, recall, F1

def evaluation_many_docs(ref_docs, pred_docs, ignore_case=True):
    assert len(ref_docs) == len(pred_docs)
    all_precision = []
    all_recall = []
    all_f1 = []
    for (ref_phrases, pred_phrases) in zip(ref_docs, pred_docs):
        if len(ref_phrases) == 0:
            continue
        a_precision, a_recall, a_f1 = evaluation_a_doc(
            ref_words=ref_phrases,
            pred_words=pred_phrases,
            ignore_case=ignore_case
        )
        all_precision.append(a_precision)
        all_recall.append(a_recall)
        all_f1.append(a_f1)
    return np.mean(all_precision), np.mean(all_recall), np.mean(all_f1)

def collect_samples():
    samples = []

    db = get_mongo_db('../config.json')
    print(db.collection_names())

    query = db['entries'].aggregate(
        [
            {
                '$match': {
                    "abstract": {"$exists": True},
                    "doi": {"$exists": True},
                    "keywords.0": {"$exists": True},
                },
            },
            {
                '$sample': {'size': 100}
            },
        ],
        allowDiskUse=True
    )
    for doc in query:
        if doc['abstract']:
            samples.append(doc)

    print('len(samples)', len(samples))

    with open('../scratch/paper_samples.json', 'w') as fw:
        json.dump(samples, fw, indent=2, default=json_util.default)


def keyword_tester(keyword_extractors, out_path='../scratch/keywords.html'):
    html_head = ''
    html_body = ''

    all_keywords = {}

    with open('../scratch/paper_samples.json', 'r') as fr:
        data = json.load(fr)

    for doc in data:
        doi = doc['doi']
        abstract = doc['abstract']
        human_keywords = doc['keywords']
        human_keywords = [w.strip() for w in human_keywords]
        human_keywords = list(filter(lambda x: len(x) > 0,  human_keywords))
        if 'human_keywords_full' not in all_keywords:
            all_keywords['human_keywords_full'] = []
        all_keywords['human_keywords_full'].append(human_keywords)

        abstract = KeywordsExtractorBase().clean_html_tag(abstract)
        if 'body_text' in doc and isinstance(doc['body_text'], list):
            for para in doc['body_text']:
                abstract+='\n{}'.format(para['Text'])

        if 'human_keywords_in_abs' not in all_keywords:
            all_keywords['human_keywords_in_abs'] = []
        all_keywords['human_keywords_in_abs'].append(list(filter(
            lambda x: x in abstract, human_keywords
        )))

        html_body += '<div style="background-color:#dbe9ea3d; font-size:20px;">\n'
        html_body += '<p>doi: {}</p>\n'.format(doi)
        html_body += '<p>human_keywords: {}</p>\n'.format(', '.join(human_keywords))
        html_body += '<p>{}</p>\n'.format(
            KeywordsExtractorBase().hightlight_keywords(
                keywords=human_keywords,
                text=abstract,
                light_color='#ffea593d',
                deep_color='#ffc107',
            )
        )

        for extractor in keyword_extractors:
            keywords = extractor.process(abstract)
            if extractor.name not in all_keywords:
                all_keywords[extractor.name] = []
            all_keywords[extractor.name].append(keywords)

            html_body += '<p>keyword extractor: {}</p>\n'.format(extractor.name)
            html_body += '<p>keywords: {}</p>\n'.format(', '.join(keywords))
            html_body += '<p>{}</p>\n'.format(
                    extractor.hightlight_keywords(
                    keywords=keywords,
                    text=abstract,
                    light_color='#ffea593d',
                    deep_color='#ffc107',
                )
            )
        html_body += '</div>\n'

    # evaluation
    # compare with human keywords
    for ref_type in ['human_keywords_full', 'human_keywords_in_abs']:
        html_body += '<div style="background-color:#dbe9ea3d; font-size:20px;">\n'
        html_body += '<h2>Compare with {}</h2>\n'.format(ref_type)
        html_body += '''<table>
            <tr>
            <th>Extractor</th>
            <th>Precision</th>
            <th>Recall</th>
            <th>F1</th>
            </tr>
        '''
        pred_types = [extractor.name for extractor in keyword_extractors]
        for p_type in pred_types:
            precision, recall, f1 = evaluation_many_docs(
                ref_docs=all_keywords[ref_type],
                pred_docs=all_keywords[p_type],
                ignore_case=True,
            )
            html_body += '''<tr>
                <td>{extractor}</td>
                <td>{precision}</td>
                <td>{recall}</td>
                <td>{f1}</td>
                </tr>
            '''.format(
                extractor=p_type,
                precision=precision,
                recall=recall,
                f1=f1,
            )

        html_body += "</table>\n"
        html_body += '</div>\n'


    with open(out_path, 'w') as fw:
        fw.writelines(
            html_body
        )




if __name__ == '__main__':
    # collect_samples()

    keyword_tester(
        keyword_extractors=[
            KeywordsExtractorSumma(
                split=True,
                scores=True
            ),
            KeywordsExtractorYake(
                name='Yake_3',
                max_ngram_size=3
            ),
            KeywordsExtractorYake(
                name='Yake_2',
                max_ngram_size=2
            ),
            KeywordsExtractorTfIdf(
                max_ngram_size=3,
            ),
            KeywordsExtractorKPMiner(),
            KeywordsExtractorSingleRank(),
            KeywordsExtractorTopicRank(),
            KeywordsExtractorTopicalPageRank(),
            KeywordsExtractorPositionRank(),
            KeywordsExtractorMultipartiteRank(),
        ],
        out_path='../scratch/keywords.html'
    )