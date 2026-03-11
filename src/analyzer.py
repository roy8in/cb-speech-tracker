"""
Central Bank Watchtower — NLP Hawk/Dove Analyzer

Primary: gtfintechlab/fomc-hawkish-dovish (RoBERTa, FOMC-specialized)
Fallback: ProsusAI/finbert (general financial sentiment)

Both models are FREE — zero API cost, runs locally on CPU.
Initial model download ~500MB, subsequent runs use cached model.

Scoring: -10 (extremely dovish) to +10 (extremely hawkish)
"""

import sys
import logging
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger(__name__)

# Topic keywords for classification
TOPIC_KEYWORDS = {
    'inflation': ['inflation', 'price stability', 'consumer prices', 'cpi', 'pce',
                   'deflation', 'disinflation', 'price pressures', 'cost of living',
                   'wage growth', 'wage pressures', 'core inflation'],
    'employment': ['employment', 'unemployment', 'labor market', 'labour market',
                    'jobs', 'payroll', 'workforce', 'hiring', 'job creation',
                    'participation rate', 'wage'],
    'growth': ['growth', 'gdp', 'economic activity', 'recession', 'expansion',
                'output', 'productivity', 'economic outlook', 'soft landing',
                'slowdown', 'contraction'],
    'financial_stability': ['financial stability', 'systemic risk', 'banking',
                             'credit conditions', 'leverage', 'asset prices',
                             'bubbles', 'financial conditions', 'stress test',
                             'contagion'],
    'monetary_policy': ['interest rate', 'policy rate', 'tightening', 'easing',
                         'quantitative easing', 'qe', 'qt', 'tapering',
                         'forward guidance', 'balance sheet', 'accommodation',
                         'restrictive', 'neutral rate', 'terminal rate'],
    'fiscal_policy': ['fiscal', 'government spending', 'debt', 'deficit',
                       'stimulus', 'tax', 'budget', 'public finance'],
    'global': ['global', 'trade', 'geopolitical', 'china', 'emerging markets',
                'supply chain', 'commodity', 'oil', 'energy', 'exchange rate',
                'currency', 'tariff'],
}


class HawkDoveAnalyzer:
    """
    Analyzes central bank speeches for hawkish/dovish sentiment.
    Uses transformer models for sentence-level classification.
    """

    def __init__(self, db=None, model_name='auto'):
        """
        Args:
            db: SpeechDB instance
            model_name: 'roberta', 'finbert', or 'auto' (try roberta first)
        """
        from .models import SpeechDB
        self.db = db or SpeechDB()
        self.model_name = model_name
        self._pipeline = None
        self._model_type = None

    def _load_model(self):
        """Lazy-load the NLP model."""
        if self._pipeline is not None:
            return

        try:
            from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
            import urllib3
            
            # --- SSL CERT VERIFICATION BYPASS FOR CORPORATE PROXY ---
            # 1. Disable urllib3 unverified HTTPS warnings
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            # 2. Force huggingface_hub to not verify SSL
            import os
            os.environ['CURL_CA_BUNDLE'] = ''
            os.environ['HTTPS_PROXY'] = ''
            os.environ['HTTP_PROXY'] = ''
            
            import huggingface_hub.utils._http as hf_http
            import requests
            
            # Monkey-patch requests in huggingface_hub to ignore SSL
            old_request = requests.Session.request
            def new_request(self, method, url, **kwargs):
                kwargs['verify'] = False
                return old_request(self, method, url, **kwargs)
            requests.Session.request = new_request
            # --------------------------------------------------------

        except ImportError:
            raise ImportError(
                "transformers library required. Install: pip install transformers torch"
            )

        if self.model_name in ('auto', 'roberta'):
            try:
                logger.info("Loading RoBERTa (fomc-hawkish-dovish) model...")
                self._pipeline = pipeline(
                    'text-classification',
                    model='gtfintechlab/fomc-hawkish-dovish',
                    tokenizer='gtfintechlab/fomc-hawkish-dovish',
                    device=-1,  # CPU
                    truncation=True,
                    max_length=512,
                )
                self._model_type = 'roberta'
                logger.info("RoBERTa model loaded successfully")
                return
            except Exception as e:
                logger.warning(f"RoBERTa model failed to load: {e}")
                if self.model_name == 'roberta':
                    raise

        # Fallback to FinBERT
        logger.info("Loading FinBERT model...")
        self._pipeline = pipeline(
            'sentiment-analysis',
            model='ProsusAI/finbert',
            tokenizer='ProsusAI/finbert',
            device=-1,
            truncation=True,
            max_length=512,
        )
        self._model_type = 'finbert'
        logger.info("FinBERT model loaded successfully")

    def _split_sentences(self, text):
        """Split text into sentences for analysis."""
        # Simple sentence splitter
        sentences = re.split(r'(?<=[.!?])\s+', text)
        # Filter: keep sentences with at least 20 chars
        return [s.strip() for s in sentences if len(s.strip()) >= 20]

    def _score_sentence(self, sentence):
        """
        Score a single sentence for hawk/dove.
        Returns (score, confidence) where score is -10 to +10.
        """
        self._load_model()

        try:
            result = self._pipeline(sentence[:512])[0]
            label = result['label'].lower()
            confidence = result['score']

            if self._model_type == 'roberta':
                # RoBERTa labels: HAWKISH, DOVISH, NEUTRAL
                if 'hawk' in label:
                    score = confidence * 10  # 0 to +10
                elif 'dov' in label:
                    score = -confidence * 10  # -10 to 0
                else:
                    score = 0.0
            else:
                # FinBERT labels: positive, negative, neutral
                # In monetary policy context:
                # positive ≈ hawkish (confident, tightening)
                # negative ≈ dovish (concerned, easing)
                if label == 'positive':
                    score = confidence * 7  # slightly lower scale for generic model
                elif label == 'negative':
                    score = -confidence * 7
                else:
                    score = 0.0

            return score, confidence

        except Exception as e:
            logger.warning(f"Scoring failed for sentence: {e}")
            return 0.0, 0.0

    def analyze(self, text, return_details=False):
        """
        Analyze a text for hawk/dove sentiment.

        Args:
            text: speech text to analyze
            return_details: if True, return per-sentence details

        Returns:
            dict with keys: score, confidence, topics, details (if requested)
        """
        if not text or len(text.strip()) < 50:
            return {'score': 0.0, 'confidence': 0.0, 'topics': {}}

        sentences = self._split_sentences(text)
        if not sentences:
            return {'score': 0.0, 'confidence': 0.0, 'topics': {}}

        # Score each sentence
        scores = []
        confidences = []
        details = []

        for sent in sentences:
            score, conf = self._score_sentence(sent)
            scores.append(score)
            confidences.append(conf)
            if return_details:
                details.append({
                    'sentence': sent[:200],
                    'score': round(score, 2),
                    'confidence': round(conf, 3),
                })

        # Weighted average (higher confidence = more weight)
        if sum(confidences) > 0:
            weighted_score = sum(s * c for s, c in zip(scores, confidences)) / sum(confidences)
            avg_confidence = sum(confidences) / len(confidences)
        else:
            weighted_score = 0.0
            avg_confidence = 0.0

        # Topic extraction
        topics = self._extract_topics(text)

        result = {
            'score': round(weighted_score, 2),
            'confidence': round(avg_confidence, 3),
            'topics': topics,
            'model': self._model_type,
            'sentences_analyzed': len(sentences),
        }

        if return_details:
            result['details'] = details

        return result

    def _extract_topics(self, text):
        """Extract topics mentioned in the text with frequency counts."""
        text_lower = text.lower()
        topics = {}

        for topic, keywords in TOPIC_KEYWORDS.items():
            count = sum(text_lower.count(kw) for kw in keywords)
            if count > 0:
                topics[topic] = count

        # Sort by frequency
        return dict(sorted(topics.items(), key=lambda x: -x[1]))

    def analyze_pending(self, limit=50):
        """
        Analyze all speeches that haven't been analyzed yet.
        Returns count of speeches analyzed.
        """
        speeches = self.db.get_speeches_without_analysis(limit=limit)
        if not speeches:
            logger.info("No pending speeches to analyze")
            return 0

        logger.info(f"Analyzing {len(speeches)} speeches...")
        analyzed = 0

        for speech in speeches:
            try:
                result = self.analyze(speech['full_text'])

                # Extract key phrases (most hawkish/dovish sentences)
                detail_result = self.analyze(speech['full_text'], return_details=True)
                key_phrases = ''
                summary = ''
                if detail_result.get('details'):
                    # Sort by absolute score (most extreme first)
                    sorted_details = sorted(
                        detail_result['details'],
                        key=lambda x: abs(x['score']),
                        reverse=True
                    )
                    top_5 = sorted_details[:5]
                    key_phrases = ' | '.join(
                        f"[{d['score']:+.1f}] {d['sentence'][:100]}"
                        for d in top_5
                    )
                    
                    # Generate extractive summary (top 3 most extreme sentences)
                    top_3 = sorted_details[:3]
                    summary_lines = []
                    for d in top_3:
                        lbl = "🦅" if d['score'] > 0 else "🕊️"
                        summary_lines.append(f"{lbl} [{d['score']:+.1f}] {d['sentence'].strip()}")
                    summary = '\n'.join(summary_lines)

                self.db.insert_analysis(
                    speech_id=speech['id'],
                    hawk_dove_score=result['score'],
                    confidence=result['confidence'],
                    topics=result['topics'],
                    key_phrases=key_phrases,
                    summary=summary,
                    model_used=result.get('model', 'unknown'),
                )
                analyzed += 1
                logger.info(
                    f"  [{speech['bank']}] {speech['title'][:50]}... "
                    f"Score: {result['score']:+.2f}"
                )

            except Exception as e:
                logger.error(f"Failed to analyze speech {speech['id']}: {e}")

        logger.info(f"Analysis complete: {analyzed}/{len(speeches)} speeches processed")
        return analyzed

    def get_stance_label(self, score):
        """Convert numeric score to human-readable label."""
        if score >= 7:
            return "🦅 Very Hawkish"
        elif score >= 3:
            return "🦅 Hawkish"
        elif score >= 1:
            return "↗️ Slightly Hawkish"
        elif score > -1:
            return "⚖️ Neutral"
        elif score > -3:
            return "↘️ Slightly Dovish"
        elif score > -7:
            return "🕊️ Dovish"
        else:
            return "🕊️ Very Dovish"


if __name__ == '__main__':
    # Quick test
    analyzer = HawkDoveAnalyzer()

    test_texts = [
        "Inflation remains our primary concern and we must act decisively to bring it under control. "
        "Further rate increases may be necessary.",

        "We need to support economic growth and employment in these challenging times. "
        "The current level of accommodation remains appropriate.",

        "The economy is growing at a moderate pace. We will continue to monitor incoming data "
        "and adjust policy as appropriate.",
    ]

    for text in test_texts:
        result = analyzer.analyze(text)
        label = analyzer.get_stance_label(result['score'])
        print(f"\n{label} (Score: {result['score']:+.2f})")
        print(f"  Text: {text[:80]}...")
        print(f"  Topics: {result['topics']}")
