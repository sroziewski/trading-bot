import nltk
from nltk.lm.preprocessing import padded_everygram_pipeline
from nltk.lm import MLE
from nltk.lm import Vocabulary


if 1 == 1:
    a = 3
else:
    b = 4


model = MLE(10)

model.perplexity()