"""
Central Bank Watchtower — Member Tracker

Tracks individual committee members' hawk/dove tendencies over time.
Supports member profiling, stance comparison, and trend detection.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger(__name__)


class MemberTracker:
    """Track and analyze individual central bank committee members."""

    def __init__(self, db=None):
        from .models import SpeechDB
        self.db = db or SpeechDB()

    def get_member_profile(self, name, bank=None):
        """
        Get a comprehensive profile of a member's stance over time.

        Returns dict with:
          - basic info (name, bank, role)
          - speech count
          - average hawk/dove score
          - recent trend (last 5 vs overall)
          - timeline
        """
        timeline = self.db.get_member_timeline(name, bank)
        if not timeline:
            return None

        scores = [t['hawk_dove_score'] for t in timeline if t['hawk_dove_score'] is not None]
        if not scores:
            return None

        avg_score = sum(scores) / len(scores)

        # Recent trend (last 5 speeches vs overall)
        recent = scores[-5:] if len(scores) >= 5 else scores
        recent_avg = sum(recent) / len(recent)
        trend = recent_avg - avg_score

        # Member info from DB
        members = self.db.get_active_members(bank)
        member_info = None
        for m in members:
            if name.lower() in m['name'].lower():
                member_info = m
                break

        return {
            'name': name,
            'bank': bank or (timeline[0]['bank'] if timeline else None),
            'role': member_info.get('role') if member_info else None,
            'speech_count': len(timeline),
            'avg_score': round(avg_score, 2),
            'recent_avg': round(recent_avg, 2),
            'trend': round(trend, 2),
            'trend_direction': 'hawkish' if trend > 0.5 else 'dovish' if trend < -0.5 else 'stable',
            'first_speech': timeline[0]['date'] if timeline else None,
            'last_speech': timeline[-1]['date'] if timeline else None,
            'timeline': timeline,
        }

    def get_bank_members_stance(self, bank):
        """
        Get current stance of all active members for a bank.

        Returns list of member profiles sorted by hawk/dove score.
        """
        members = self.db.get_active_members(bank)
        profiles = []

        for member in members:
            name = member['name']
            timeline = self.db.get_member_timeline(name, bank)
            if not timeline:
                continue

            scores = [t['hawk_dove_score'] for t in timeline if t['hawk_dove_score'] is not None]
            if not scores:
                continue

            recent = scores[-3:] if len(scores) >= 3 else scores
            recent_avg = sum(recent) / len(recent)

            profiles.append({
                'name': name,
                'role': member.get('role', ''),
                'is_voter': member.get('is_voter', 1),
                'avg_score': round(sum(scores) / len(scores), 2),
                'recent_score': round(recent_avg, 2),
                'speech_count': len(scores),
                'last_speech': timeline[-1]['date'],
            })

        # Sort by recent score (most hawkish first)
        profiles.sort(key=lambda x: -x['recent_score'])
        return profiles

    def get_bank_average_stance(self, bank, weighted_by_recency=True):
        """
        Calculate the overall stance of a central bank.

        Args:
            bank: bank code
            weighted_by_recency: give more weight to recent speeches

        Returns dict with average score, trend, member breakdown
        """
        stance_data = self.db.get_bank_stance(bank, limit=50)
        if not stance_data:
            return None

        if weighted_by_recency:
            # More recent speeches get higher weight
            total_weight = 0
            weighted_sum = 0
            for i, s in enumerate(stance_data):
                weight = 1.0 / (i + 1)  # 1.0, 0.5, 0.33, ...
                weighted_sum += s['hawk_dove_score'] * weight
                total_weight += weight
            avg_score = weighted_sum / total_weight if total_weight > 0 else 0
        else:
            scores = [s['hawk_dove_score'] for s in stance_data]
            avg_score = sum(scores) / len(scores)

        return {
            'bank': bank,
            'avg_score': round(avg_score, 2),
            'num_speeches': len(stance_data),
            'last_speech_date': stance_data[0]['date'] if stance_data else None,
        }

    def get_global_stance_summary(self):
        """
        Get a summary of all 6 central banks' current stances.
        """
        from .models import SpeechDB
        banks = SpeechDB.BANKS
        summary = []

        for bank in banks:
            stance = self.get_bank_average_stance(bank)
            if stance:
                summary.append(stance)

        # Sort by score (most hawkish first)
        summary.sort(key=lambda x: -x['avg_score'])
        return summary

    def detect_stance_shifts(self, bank, threshold=2.0, window=5):
        """
        Detect significant shifts in a bank's stance.

        Args:
            bank: bank code
            threshold: minimum score change to flag
            window: number of recent speeches to compare

        Returns list of detected shifts
        """
        stance_data = self.db.get_bank_stance(bank, limit=50)
        if len(stance_data) < window * 2:
            return []

        recent = stance_data[:window]
        previous = stance_data[window:window * 2]

        recent_avg = sum(s['hawk_dove_score'] for s in recent) / len(recent)
        previous_avg = sum(s['hawk_dove_score'] for s in previous) / len(previous)

        shift = recent_avg - previous_avg
        if abs(shift) >= threshold:
            return [{
                'bank': bank,
                'shift': round(shift, 2),
                'direction': 'hawkish' if shift > 0 else 'dovish',
                'recent_avg': round(recent_avg, 2),
                'previous_avg': round(previous_avg, 2),
                'period': f"{recent[-1]['date']} to {recent[0]['date']}",
            }]
        return []

    def format_stance_emoji(self, score):
        """Get emoji representation of stance."""
        if score >= 7:
            return "🦅🦅🦅"
        elif score >= 3:
            return "🦅🦅"
        elif score >= 1:
            return "🦅"
        elif score > -1:
            return "⚖️"
        elif score > -3:
            return "🕊️"
        elif score > -7:
            return "🕊️🕊️"
        else:
            return "🕊️🕊️🕊️"
