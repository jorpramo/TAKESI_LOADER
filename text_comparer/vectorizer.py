from collections import defaultdict
import re

from text_comparer.similarity import similarity
from nltk.corpus import stopwords

def word_frequencies(word_vector):
    """What percent of the time does each word in the vector appear?

    Returns a dictionary mapping each word to its frequency.

    """
    num_words = len(word_vector)
    frequencies = defaultdict(float)
    for word in word_vector:
        frequencies[word] += 1.0 / num_words

    return dict(frequencies)


def compare_vectors(word_vector1, word_vector2):
    """Numerical similarity between lists of words. Higher is better.

    Uses cosine similarity.
    Result range: 0 (bad) - 1 (uses all the same words in the same proportions)

    """
    all_words = list(set(word_vector1).union(set(word_vector2)))
    frequency_dict1 = word_frequencies(word_vector1)
    frequency_dict2 = word_frequencies(word_vector2)

    frequency_vector1 = [frequency_dict1.get(word, 0) for word in all_words]
    frequency_vector2 = [frequency_dict2.get(word, 0) for word in all_words]

    return similarity(frequency_vector1, frequency_vector2)


def vectorize_text(text):
    """Takes in text, processes it, and vectorizes it."""

    def remove_punctuation(text):
        """Removes special characters from text."""
        return re.sub('[,.?";:\-!@#$%^&*()]', '', text)

    def remove_common_words(text_vector):
        """Removes 50 most common words in the uk english.

        source: http://www.bckelk.ukfsn.org/words/uk1000n.html

        """
        spanish_stops = set(stopwords.words('spanish'))
        return [word for word in text_vector if word not in spanish_stops]

    text = text.lower()
    text = remove_punctuation(text)
    words_list = text.split()
    words_list = remove_common_words(words_list)

    return words_list


def compare_texts(text1, text2):
    """How similar are the two input paragraphs?"""
    return compare_vectors(vectorize_text(text1), vectorize_text(text2))
