from nltk.sentiment.vader import SentimentIntensityAnalyzer

def analyze_sentiment(review):
  analyzer = SentimentIntensityAnalyzer()
  scores = analyzer.polarity_scores(review)
  
  label = 'Positive' if scores['compound'] >= 0.3 else 'Negative' if scores['compound'] <= -0.3 else 'Neutral'

  return scores['compound'], label


class SentimentAnalyzer:
  def __init__(self, positive_threshold=0.3, negative_threshold=-0.3):
    self.analyzer = SentimentIntensityAnalyzer()
    self.positive_threshold = positive_threshold
    self.negative_threshold = negative_threshold

  def analyze_sentiment(self, review):
    scores = self.analyzer.polarity_scores(review)

    label = (
      'Positive' if scores['compound'] >= self.positive_threshold 
      else 'Negative' if scores['compound'] <= self.negative_threshold 
      else 'Neutral'
    )

    return scores['compound'], label