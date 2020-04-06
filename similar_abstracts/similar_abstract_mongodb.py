import os
import heapq
import pymongo
import logging
import fasttext
import datetime
import subprocess
import numpy as np

from pretokenize import PreTokenize

logger = logging.getLogger(__name__)


def estimate_epoch(file_size, time=100):
    """
    a empirical method to estimate number of iterations depend on the size of corpus
    """
    slope = 1.75e-8
    epoch = time / file_size / slope
    if epoch <= 20:
        epoch = 20
    return epoch


class AbstractSimilarity:
    # general configurations
    n = 3  # the number of relevant abstracts to store

    collection = "entries"  # collection to be updated

    # entry names
    abstract_entry = "abstract"  # abstract_text
    similar_abstracts_entry = "similar_abstracts"  # relevant abstracts' doi and similarity
    doi_entry = "doi"  # doi
    abstract_vec_entry = "abstract_vec"  # abstract_vec

    # tmp_dir
    tmp_dir = r"/var/tmp"  # used for storing tmp file when training
    if not os.path.isdir(tmp_dir):  # if not exists, use current directory
        tmp_dir = os.getcwd()

    # training args
    training_args = {
        "model": "skipgram",  # cbow or skipgram, usually skipgram is better
        "lr": 0.05,  # learning rate
        "dim": 300,  # vector dim
        "ws": 5,  # window size
        "epoch": 0,  # iteration number
        "minCount": 5,
        "minn": 3,
        "maxn": 6,
        "verbose": 0
    }

    def __init__(self, model_path):
        client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                                     password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
        self.db = client[os.getenv("COVID_DB")]
        logger.info("Log in to the database successfully.")
        self.model_path = model_path
        try:
            self.model = fasttext.load_model(self.model_path)
        except (ValueError, FileNotFoundError):
            logger.warning("No proper model file found. Please run train method first.")
            self.model = None

    def train(self):
        """
        update the language model
        """
        self.model = None  # remove the old model (for saving memory)
        
        fasttext_dir = os.getenv("FASTTEXT_DIR")

        if fasttext_dir is None:
            raise TypeError("Env variable FASTTEXT_DIR should be set first.")

        current_time = datetime.datetime.now()
        file_name = "fasttext_{hash_code}_{year}_{month}_{day}".format(hash_code=abs(hash(current_time)),
                                                                       year=current_time.year,
                                                                       month=current_time.month,
                                                                       day=current_time.day)
        tmp_path = os.path.join(self.tmp_dir, file_name)

        # make corpus
        logger.info("Starting to build corpus for training, tmp file: {}".format(tmp_path))
        with open(tmp_path, "w", encoding="utf-8") as f:
            for doc in self.db[self.collection].find({self.abstract_entry: {"$exists": True, "$ne": None}}):
                tokens = PreTokenize.tokenize(doc.get(self.abstract_entry, ""), True)
                if tokens:
                    f.write(" ".join(tokens)+"\n")

        self.training_args["epoch"] = self.training_args["epoch"] or estimate_epoch(os.path.getsize(tmp_path))
        logger.info("Training the model -- Arguments: {}".format(self.training_args))

        try:
            # training the model
            subprocess.run([
                os.path.join(os.getenv("FASTTEXT_DIR"), "fasttext"),
                "{model}".format(**self.training_args),
                "-input", "{}".format(tmp_path),
                "-output", "{}".format(self.model_path.rstrip(".bin")),
                "-maxn", "{maxn}".format(**self.training_args),
                "-minn", "{minn}".format(**self.training_args),
                "-lr", "{lr}".format(**self.training_args),
                "-dim", "{dim}".format(**self.training_args),
                "-epoch", "{epoch}".format(**self.training_args),
                "-minCount", "{minCount}".format(**self.training_args),
                "-ws", "{ws}".format(**self.training_args)
            ], check=True)
            self.model = fasttext.load_model(self.model_path)  # load new model
        finally:
            # delete the tmp file
            if os.path.isfile(tmp_path):
                os.remove(tmp_path)
            if os.path.isfile(self.model_path.rstrip(".bin")+".vec"):
                os.remove(self.model_path.rstrip(".bin")+".vec")
        logger.info("Successfully save the new model and remove tmp file")
        self.db.metadata.update_one(
            {"data": "last_word_embedding_trained"}, {"$set": {"datetime": datetime.datetime.now()}}
        )

    def build(self):
        """
        build similar abstracts entries from scratch
        for initialize the database or after changing the model
        """
        # clear all the similar_abstract_entry
        self.db[self.collection].update({}, {"$unset": {self.similar_abstracts_entry: []}})
        self.db[self.collection].update({}, {"$unset": {self.abstract_vec_entry: None}})

        current_time = datetime.datetime.now()

        cursor1 = {self.abstract_entry: {"$exists": True}}
        cursor2 = cursor1
        # do update routine
        self._update(cursor1, cursor2)

        # log the update
        self.db.metadata.update_one(
            {"data": "last_abstract_similarity_sweep"}, {"$set": {"datetime": current_time}}, upsert=True
        )

    def update(self):
        """
        update the database with the newly added doc
        For faster speed, we employ some tricks:

                           old papers          new papers
                   _______________________________________
                   |                        |            |
                   |                        |            |
                   |                        |            |
                   |                        |            |
              old  |                        |      a     |
             paper |                        |            |
                   |                        |            |
                   |                        |            |
                   |                        |            |
                   |________________________|____________|
                   |                        |            |
              new  |                        |            |
             paper |            a'          |      b     |
                   |                        |            |
                   |________________________|____________|

        Considering the similarity matrix M above.
        M[i][j] is the cosine similarity between ith and jth abstract vector.
        Obviously, M is a symmetric matrix.
        And we have to compare one abstract with all other abstracts at least onece.
        Notice that when some new papers are added to the database, the matrix will have three new parts,
        namely, a, a' and b.
        a and a' are symmetric, so we only need to calculate a & b.

        As for b, if we only compute the lower triangle of b, half of the time will be saved.
        Here we use {"_id": {"$lt": doc["_id"]}} filter to do that.
        """

        last_abstract_similarity_sweep = self.db.metadata.find_one(
            {'data': 'last_abstract_similarity_sweep'}
        )['datetime']

        current_time = datetime.datetime.now()  # the routine may take very long time

        # compute part b
        cursor1 = {self.abstract_entry: {"$exists": True}, "_bt": {"$gte": last_abstract_similarity_sweep}}
        cursor2 = cursor1
        self._update(cursor1, cursor2)

        # compute part a
        cursor2 = {self.abstract_entry: {"$exists": True}, "_bt": {"$lt": last_abstract_similarity_sweep}}
        self._update(cursor1, cursor2)

        # log the update
        self.db.metadata.update_one(
            {"data": "last_abstract_similarity_sweep"}, {"$set": {"datetime": current_time}}
        )

    def _update(self, cursor1, cursor2):
        """
        general method for generating similar abstracts entry
        if cursor1 and cursor2 are the same, we can reduce the computation from n ^ 2 to \frac{1}{2} n ^ 2

        :type cursor1: dict
        :type cursor2: dict
        """
        if cursor1 == cursor2:
            cursor2 = None

        # for each abstracts to be updated
        for doc in self.db[self.collection].find(cursor1):
            try:
                abstract, similar_abstracts, doi, abstract_vec, vec_norm = self._get_para_info(doc)
            except TypeError:
                continue

            # for each abstract in the database
            if cursor2 is None:
                _cursor2 = self.db[self.collection].find(
                    dict(cursor1, _id={"$lt": doc["_id"]})
                )
            else:
                _cursor2 = self.db[self.collection].find(cursor2)
            for doc_ in _cursor2:
                try:
                    abstract_, similar_abstracts_, doi_, abstract_vec_, vec_norm_ = self._get_para_info(doc_)
                except TypeError:
                    continue

                # calculate cosine similarity
                similarity = (abstract_vec @ abstract_vec_) / (vec_norm * vec_norm_)
                if similarity >= 0.99:  # when doc and doc_ are exactly the same
                    continue
                similarity = float(similarity)
                # update the best match list
                if len(similar_abstracts) < self.n:
                    similar_abstracts.append([similarity, doi_])
                    heapq.heapify(similar_abstracts)
                else:
                    heapq.heappushpop(similar_abstracts, [similarity, doi_])
                if len(similar_abstracts_) < self.n:
                    similar_abstracts_.append([similarity, doi])
                    heapq.heapify(similar_abstracts_)
                else:
                    heapq.heappushpop(similar_abstracts_, [similarity, doi])
                self.db[self.collection].update_one({self.doi_entry: doi_},
                                                    {"$set": {self.similar_abstracts_entry: similar_abstracts_}},
                                                    upsert=True)

            self.db[self.collection].update_one({self.doi_entry: doi},
                                                {"$set": {self.similar_abstracts_entry: similar_abstracts}},
                                                upsert=True)

    def _get_para_vec(self, abstract_text, restrict_min_token_num=True):
        """
        tokenize the text, then generate the input vector
        :param abstract_text: input vector
        :type abstract_text: str
        :param restrict_min_token_num: when True, it will return None when the number of valid tokens is too low.
        :type restrict_min_token_num: bool
        :return: abstract_vec, vec_norm
        """
        # tokenize
        abstract_tokens = PreTokenize.tokenize(abstract_text, restrict_min_token_num)
        if not abstract_tokens:
            return None, None  # must carefully handle this situation

        # get vec
        abstract_vec = self.model.get_sentence_vector(" ".join(abstract_tokens))
        vec_norm = np.sqrt(abstract_vec @ abstract_vec)

        return abstract_vec, vec_norm

    def _get_para_info(self, doc):
        """
        get necessary information from the doc object
        :param doc: dict-like object
        :return: abstract, similar_abstracts, doi, abstract_vec, vec_norm
        """
        abstract = doc.get(self.abstract_entry, "") or ""
        similar_abstracts = doc.get(self.similar_abstracts_entry, []) or []
        doi = doc.get(self.doi_entry, "") or ""

        if not doi or not abstract:
            return None

        tmp = doc.get(self.abstract_vec_entry, None)
        no_vec = tmp is None
        if no_vec:
            tmp = self._get_para_vec(abstract, False)
        if tmp[0] is None:
            logger.info("{} cannot be tokenized.".format(doi))
            return None
        if no_vec:
            byte_array = (tmp[0].tobytes(), tmp[1].tobytes())
            self.db[self.collection].update_one({"doi": doi}, {"$set": {self.abstract_vec_entry: byte_array}},
                                                upsert=True)
        abstract_vec, vec_norm = tmp
        abstract_vec = np.frombuffer(abstract_vec, dtype=np.float32)
        vec_norm = np.frombuffer(vec_norm, dtype=np.float32)
        return abstract, similar_abstracts, doi, abstract_vec, vec_norm


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("mode", help="Possible modes: train, build, update.", choices=["train", "build", "update"])
    parser.add_argument("-m", "--model", help="path to the fasttext model.", required=True)
    parser.add_argument("-v", "--verbose", help="set logger level, default=WARNING", default="WARNING",
                        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])

    args = parser.parse_args()
    mode = args.mode
    model_path = args.model
    verbose = args.verbose

    logger.setLevel(verbose)

    aa = AbstractSimilarity(model_path)
    if mode == "train":
        aa.train()
    elif mode == "build":
        aa.build()
    elif mode == "update":
        aa.update()
