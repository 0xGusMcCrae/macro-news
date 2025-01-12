from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import anthropic
from dataclasses import dataclass
import logging
from textblob import TextBlob
import numpy as np

logger = logging.getLogger(__name__)

@dataclass
class FedAnalysis:
    """Analysis results for Fed communications"""
    hawkish_score: float  # -1 (very dovish) to 1 (very hawkish)
    confidence: float     # 0 to 1
    key_themes: List[str]
    policy_bias: str     # 'hawkish', 'dovish', 'neutral'
    forward_guidance: Dict[str, str]
    market_implications: Dict[str, str]
    strategic_intent: Dict[str, str]

class FedAnalyzer:
    """Analyzes Federal Reserve communications"""

    def __init__(self, config: Dict[str, str]):
        self.client = anthropic.Client(api_key=config['anthropic_key'])
        self.market_context = {}
        self.prior_communications = []

    async def analyze_speech(self, 
                           speech_text: str,
                           speaker: str,
                           title: str,
                           date: datetime,
                           market_context: Dict[str, Any]) -> FedAnalysis:
        """Analyze a Fed speech considering market context"""
        
        # Update context
        self.market_context = market_context
        
        # Get AI analysis
        ai_analysis = await self._get_claude_analysis(
            speech_text, speaker, title, date
        )
        
        # Compute quantitative metrics
        metrics = self._compute_speech_metrics(speech_text)
        
        # Analyze strategic communication
        strategic = self._analyze_strategic_intent(
            speech_text, ai_analysis, speaker
        )
        
        # Create comprehensive analysis
        analysis = FedAnalysis(
            hawkish_score=metrics['hawkish_score'],
            confidence=metrics['confidence'],
            key_themes=ai_analysis['key_themes'],
            policy_bias=self._determine_policy_bias(metrics['hawkish_score']),
            forward_guidance=ai_analysis['forward_guidance'],
            market_implications=ai_analysis['market_implications'],
            strategic_intent=strategic
        )
        
        # Update prior communications
        self.prior_communications.append({
            'date': date,
            'speaker': speaker,
            'analysis': analysis
        })
        
        return analysis

    async def _get_claude_analysis(self,
                                 speech_text: str,
                                 speaker: str,
                                 title: str,
                                 date: datetime) -> Dict[str, Any]:
        """Get AI analysis of speech"""
        
        context_string = self._format_market_context()
        prior_comms = self._format_prior_communications()
        
        prompt = f"""Analyze this Federal Reserve communication with focus on policy implications and strategic messaging:

Speech Details:
- Speaker: {speaker}
- Title: {title}
- Date: {date}

Market Context:
{context_string}

Recent Fed Communications:
{prior_comms}

Speech Text:
{speech_text}

Please analyze:

1. Direct Communication:
- Key themes and messages
- Policy signals
- Economic outlook
- Risk assessment

2. Strategic Communication:
- How is this trying to influence market expectations?
- What's the strategic intent behind the messaging?
- How does this fit with or differ from recent Fed communications?
- Is there deliberate ambiguity or clear signaling?

3. Market Implications:
- Impact on rate expectations
- Asset price implications
- Risk appetite effects
- Yield curve implications

Focus particularly on:
- Forward guidance
- Changes in tone from previous communications
- Strategic use of communication as a policy tool"""

        response = self.client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2000,
            temperature=0,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        
        # Process and structure the response
        # (In practice, you might want to use more sophisticated parsing)
        analysis = {
            'key_themes': self._extract_key_themes(response.content),
            'forward_guidance': self._extract_forward_guidance(response.content),
            'market_implications': self._extract_market_implications(response.content)
        }
        
        return analysis

    def _compute_speech_metrics(self, speech_text: str) -> Dict[str, float]:
        """Compute quantitative metrics from speech"""
        text = speech_text.lower()
        
        # Define keyword dictionaries with weights
        hawkish_terms = {
            'inflation risk': 2.0,
            'price stability': 1.5,
            'vigilant': 1.5,
            'restrictive': 2.0,
            'higher rates': 1.5,
            'upside risk': 1.0
        }
        
        dovish_terms = {
            'patient': -1.0,
            'accommodative': -2.0,
            'gradual': -1.0,
            'downside risk': -1.0,
            'carefully': -0.5,
            'mindful': -0.5
        }

        # Calculate weighted score
        total_score = 0
        total_weight = 0
        
        for term, weight in hawkish_terms.items():
            count = text.count(term)
            total_score += count * weight
            total_weight += count

        for term, weight in dovish_terms.items():
            count = text.count(term)
            total_score += count * weight
            total_weight += count
            
        # Normalize score and compute confidence
        hawkish_score = np.clip(total_score / max(total_weight, 1), -1, 1)
        confidence = min(total_weight / 10, 1)  # Scale confidence based on term matches

        return {
            'hawkish_score': hawkish_score,
            'confidence': confidence
        }

    def _analyze_strategic_intent(self,
                                speech_text: str,
                                ai_analysis: Dict[str, Any],
                                speaker: str) -> Dict[str, str]:
        """Analyze strategic communication intent"""
        # Consider market positioning
        market_expectations = self.market_context.get('rate_expectations', {})
        market_pricing = self.market_context.get('market_pricing', {})
        
        # Compare with recent communications
        comm_shift = self._analyze_communication_shift()
        
        # Determine if pushing back against market views
        pushing_back = self._is_pushing_back_against_market(
            ai_analysis['forward_guidance'],
            market_expectations
        )
        
        return {
            'primary_intent': self._determine_primary_intent(
                ai_analysis, pushing_back
            ),
            'expectation_management': pushing_back,
            'communication_shift': comm_shift,
            'strategic_clarity': self._assess_strategic_clarity(ai_analysis)
        }

    def _format_market_context(self) -> str:
        """Format market context for analysis"""
        context = []
        
        if 'rate_expectations' in self.market_context:
            context.append(f"Rate Expectations: {self.market_context['rate_expectations']}")
        
        if 'market_pricing' in self.market_context:
            context.append(f"Market Pricing: {self.market_context['market_pricing']}")
            
        return "\n".join(context)

    def _format_prior_communications(self) -> str:
        """Format recent Fed communications"""
        recent = sorted(
            self.prior_communications[-3:],
            key=lambda x: x['date'],
            reverse=True
        )
        
        return "\n".join([
            f"- {c['date'].strftime('%Y-%m-%d')}: {c['speaker']} "
            f"(Score: {c['analysis'].hawkish_score:.2f})"
            for c in recent
        ])

    def _determine_policy_bias(self, hawkish_score: float) -> str:
        """Determine policy bias based on hawkish score"""
        if hawkish_score > 0.3:
            return 'hawkish'
        elif hawkish_score < -0.3:
            return 'dovish'
        else:
            return 'neutral'
        
    def _extract_key_themes(self, claude_response: str) -> List[str]:
        """Extract key themes from Claude's analysis"""
        try:
            # Look for key themes section in the response
            themes_section = claude_response.split('Key themes and messages')[1].split('Policy signals')[0]
            
            # Extract bullet points or main ideas
            themes = []
            for line in themes_section.split('\n'):
                line = line.strip()
                if line.startswith('-') or line.startswith('•'):
                    themes.append(line.lstrip('- •').strip())
            
            return themes[:5]  # Return top 5 themes
        except Exception as e:
            logger.error(f"Error extracting key themes: {str(e)}")
            return ['error extracting themes']

    def _extract_forward_guidance(self, claude_response: str) -> Dict[str, str]:
        """Extract forward guidance statements and implications"""
        try:
            guidance = {
                'policy_path': '',
                'timeline': '',
                'conditions': '',
                'risks': ''
            }
            
            # Look for forward guidance and policy sections
            for section in ['Forward guidance', 'Policy signals', 'Policy implications']:
                if section in claude_response:
                    section_text = claude_response.split(section)[1].split('\n\n')[0]
                    
                    if 'path' in section_text.lower():
                        guidance['policy_path'] = section_text.strip()
                    if 'timeline' in section_text.lower() or 'when' in section_text.lower():
                        guidance['timeline'] = section_text.strip()
                    if 'condition' in section_text.lower() or 'depend' in section_text.lower():
                        guidance['conditions'] = section_text.strip()
                    if 'risk' in section_text.lower():
                        guidance['risks'] = section_text.strip()
            
            return guidance
        except Exception as e:
            logger.error(f"Error extracting forward guidance: {str(e)}")
            return {}

    def _extract_market_implications(self, claude_response: str) -> Dict[str, str]:
        """Extract market implications from analysis"""
        try:
            implications = {
                'rates': '',
                'equities': '',
                'fixed_income': '',
                'currencies': '',
                'overall_risk': ''
            }
            
            # Find market implications section
            if 'Market Implications' in claude_response:
                market_section = claude_response.split('Market Implications')[1].split('\n\n')[0]
                
                # Parse different market aspects
                for line in market_section.split('\n'):
                    line = line.strip()
                    if 'rate' in line.lower():
                        implications['rates'] = line
                    elif any(x in line.lower() for x in ['equity', 'stock', 'index']):
                        implications['equities'] = line
                    elif any(x in line.lower() for x in ['bond', 'yield', 'treasury']):
                        implications['fixed_income'] = line
                    elif any(x in line.lower() for x in ['currency', 'fx', 'dollar']):
                        implications['currencies'] = line
                    elif any(x in line.lower() for x in ['risk', 'sentiment']):
                        implications['overall_risk'] = line
            
            return implications
        except Exception as e:
            logger.error(f"Error extracting market implications: {str(e)}")
            return {}

    def _analyze_communication_shift(self) -> str:
        """Analyze shift in communication tone from previous statements"""
        try:
            if not self.prior_communications:
                return "insufficient_history"
                
            # Get average hawkish score from last 3 communications
            recent_scores = [
                comm['analysis'].hawkish_score 
                for comm in self.prior_communications[-3:]
            ]
            avg_score = sum(recent_scores) / len(recent_scores)
            
            # Determine shift
            latest_score = recent_scores[-1]
            shift = latest_score - avg_score
            
            if abs(shift) < 0.1:
                return "consistent"
            elif shift > 0:
                return "more_hawkish"
            else:
                return "more_dovish"
                
        except Exception as e:
            logger.error(f"Error analyzing communication shift: {str(e)}")
            return "unknown"

    def _is_pushing_back_against_market(
        self, 
        forward_guidance: Dict[str, str],
        market_expectations: Dict[str, Any]
    ) -> bool:
        """Determine if Fed is pushing back against market expectations"""
        try:
            # Get market-implied rate path
            market_cuts = market_expectations.get('rate_cuts_next_12m', 0)
            market_hikes = market_expectations.get('rate_hikes_next_12m', 0)
            
            # Analyze forward guidance language
            guidance_text = ' '.join(forward_guidance.values()).lower()
            
            # Check for pushback signals
            if market_cuts > 0 and any(x in guidance_text for x in [
                'premature to ease',
                'higher for longer',
                'restrictive stance',
                'inflation risks'
            ]):
                return True
                
            if market_hikes > 0 and any(x in guidance_text for x in [
                'appropriate stance',
                'well positioned',
                'monitoring incoming data',
                'patient approach'
            ]):
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error analyzing market pushback: {str(e)}")
            return False

    def _determine_primary_intent(
        self,
        ai_analysis: Dict[str, Any],
        pushing_back: bool
    ) -> str:
        """Determine primary communication intent"""
        try:
            guidance = ai_analysis.get('forward_guidance', {})
            implications = ai_analysis.get('market_implications', {})
            
            # Check for clear policy signals
            if any(x in str(guidance) for x in ['will', 'must', 'need to', 'committed']):
                return 'policy_commitment'
                
            # Check if managing expectations
            if pushing_back:
                return 'expectation_management'
                
            # Check if providing information update
            if 'data' in str(guidance) or 'incoming information' in str(guidance):
                return 'information_update'
                
            # Check if risk communication
            if 'risk' in str(guidance) or 'uncertainty' in str(guidance):
                return 'risk_communication'
                
            return 'general_communication'
            
        except Exception as e:
            logger.error(f"Error determining primary intent: {str(e)}")
            return 'unknown'

    def _assess_strategic_clarity(self, ai_analysis: Dict[str, Any]) -> str:
        """Assess level of strategic clarity in communication"""
        try:
            guidance = ai_analysis.get('forward_guidance', {})
            text = ' '.join(str(v) for v in guidance.values())
            
            # Count clear vs ambiguous phrases
            clear_phrases = [
                'will', 'must', 'need to', 'committed to',
                'clear', 'certain', 'definitely', 'strongly'
            ]
            
            ambiguous_phrases = [
                'may', 'might', 'could', 'perhaps',
                'monitor', 'assess', 'evaluate', 'consider'
            ]
            
            clear_count = sum(text.count(phrase) for phrase in clear_phrases)
            ambiguous_count = sum(text.count(phrase) for phrase in ambiguous_phrases)
            
            if clear_count > ambiguous_count * 2:
                return 'very_clear'
            elif clear_count > ambiguous_count:
                return 'somewhat_clear'
            elif clear_count < ambiguous_count * 2:
                return 'very_ambiguous'
            else:
                return 'somewhat_ambiguous'
                
        except Exception as e:
            logger.error(f"Error assessing strategic clarity: {str(e)}")
            return 'unknown'