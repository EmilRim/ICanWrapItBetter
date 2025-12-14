import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import glob

class SpotifyAnalyzer:
    def __init__(self, data_folder):
        self.data_folder = data_folder
        self.streams = []
        self.session_gap_minutes = 30
        self.completion_threshold = 0.90  # 90% = considered complete
        
    def load_streaming_history(self):
        """Load all StreamingHistory*.json files"""
        pattern = os.path.join(self.data_folder, "StreamingHistory_music*.json")
        files = glob.glob(pattern)
        
        print(f"Found {len(files)} streaming history files")
        
        for file in sorted(files):
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.streams.extend(data)
        
        # Filter from December 2024 onwards
        cutoff = datetime(2024, 12, 1)
        self.streams = [s for s in self.streams if datetime.strptime(s['endTime'], '%Y-%m-%d %H:%M') >= cutoff]
        
        # Sort by timestamp
        self.streams.sort(key=lambda x: x['endTime'])
        
        print(f"Loaded {len(self.streams)} streams from December 2024 onwards")
        
    def get_song_key(self, stream):
        """Create unique key for song"""
        track = stream.get('trackName', 'Unknown')
        artist = stream.get('artistName', 'Unknown')
        return f"{track}|||{artist}"
    
    def analyze_basic_metrics(self):
        """Calculate play counts and total minutes"""
        song_stats = defaultdict(lambda: {
            'play_count': 0,
            'total_ms': 0,
            'track_name': '',
            'artist_name': ''
        })
        
        for stream in self.streams:
            key = self.get_song_key(stream)
            song_stats[key]['play_count'] += 1
            song_stats[key]['total_ms'] += stream.get('msPlayed', 0)
            
            # Store names
            if not song_stats[key]['track_name']:
                song_stats[key]['track_name'] = stream.get('trackName', 'Unknown')
                song_stats[key]['artist_name'] = stream.get('artistName', 'Unknown')
        
        return song_stats
    
    def detect_session_starters(self):
        """Find songs that start listening sessions"""
        session_starters = defaultdict(int)
        
        for i, stream in enumerate(self.streams):
            if i == 0:
                # First song is a session starter
                key = self.get_song_key(stream)
                session_starters[key] += 1
                continue
            
            # Check time gap from previous song
            current_time = datetime.strptime(stream['endTime'], '%Y-%m-%d %H:%M')
            prev_time = datetime.strptime(self.streams[i-1]['endTime'], '%Y-%m-%d %H:%M')
            
            gap = (current_time - prev_time).total_seconds() / 60  # minutes
            
            if gap >= self.session_gap_minutes:
                key = self.get_song_key(stream)
                session_starters[key] += 1
        
        print(f"Detected {sum(session_starters.values())} listening sessions")
        return session_starters
    
    def calculate_completion_rates(self):
        """Calculate how often songs are played to completion"""
        song_completions = defaultdict(lambda: {
            'total_plays': 0,
            'completed_plays': 0,
            'skipped_plays': 0,
            'completion_rate': 0.0
        })
        
        for stream in self.streams:
            key = self.get_song_key(stream)
            ms_played = stream.get('msPlayed', 0)
            
            song_completions[key]['total_plays'] += 1
            
            # Consider it completed if played for more than 90 seconds
            if ms_played > 90000:  # 90 seconds
                song_completions[key]['completed_plays'] += 1
            else:
                song_completions[key]['skipped_plays'] += 1
        
        # Calculate rates
        for key in song_completions:
            stats = song_completions[key]
            if stats['total_plays'] > 0:
                stats['completion_rate'] = stats['completed_plays'] / stats['total_plays']
        
        return song_completions
    
    def calculate_listening_density(self):
        """Calculate how many days had multiple listens of same song"""
        daily_listens = defaultdict(lambda: defaultdict(int))
        
        for stream in self.streams:
            key = self.get_song_key(stream)
            date = datetime.strptime(stream['endTime'], '%Y-%m-%d %H:%M').date()
            daily_listens[key][date] += 1
        
        density_scores = {}
        density_details = {}
        for song_key, dates in daily_listens.items():
            # Count days with 2+ listens (shows obsession)
            multi_listen_days = sum(1 for count in dates.values() if count >= 2)
            max_in_one_day = max(dates.values()) if dates else 0
            density_scores[song_key] = multi_listen_days
            density_details[song_key] = {
                'multi_listen_days': multi_listen_days,
                'max_in_one_day': max_in_one_day
            }
        
        return density_scores, density_details
    
    def calculate_consistency(self):
        """Calculate how many different weeks/months song appeared in"""
        song_weeks = defaultdict(set)
        song_months = defaultdict(set)
        
        for stream in self.streams:
            key = self.get_song_key(stream)
            date = datetime.strptime(stream['endTime'], '%Y-%m-%d %H:%M')
            week = f"{date.year}-W{date.isocalendar()[1]}"
            month = f"{date.year}-{date.month:02d}"
            song_weeks[key].add(week)
            song_months[key].add(month)
        
        consistency_scores = {k: len(v) for k, v in song_weeks.items()}
        consistency_details = {k: {
            'weeks': len(song_weeks[k]),
            'months': len(song_months[k])
        } for k in song_weeks.keys()}
        
        return consistency_scores, consistency_details
    
    def find_peak_month(self, song_key):
        """Find which month had the most listens for a song"""
        monthly_counts = defaultdict(int)
        
        for stream in self.streams:
            key = self.get_song_key(stream)
            if key == song_key:
                date = datetime.strptime(stream['endTime'], '%Y-%m-%d %H:%M')
                month = date.strftime('%B')  # Full month name
                monthly_counts[month] += 1
        
        if monthly_counts:
            peak_month = max(monthly_counts, key=monthly_counts.get)
            peak_count = monthly_counts[peak_month]
            return peak_month, peak_count
        return None, 0
    
    def calculate_weighted_score(self, song_key, basic_stats, session_starters, 
                                 completion_rates, density_scores, consistency_scores):
        """Calculate weighted score for a song"""
        
        # Normalize factors (0-1 scale)
        max_minutes = max(s['total_ms'] for s in basic_stats.values()) / 60000
        max_sessions = max(session_starters.values()) if session_starters else 1
        max_density = max(density_scores.values()) if density_scores else 1
        max_consistency = max(consistency_scores.values()) if consistency_scores else 1
        
        # Get song metrics
        minutes = basic_stats[song_key]['total_ms'] / 60000
        sessions = session_starters.get(song_key, 0)
        completion = completion_rates.get(song_key, {}).get('completion_rate', 0)
        density = density_scores.get(song_key, 0)
        consistency = consistency_scores.get(song_key, 0)
        
        # Individual component scores (0-1)
        minutes_score = minutes / max_minutes
        sessions_score = sessions / max_sessions
        completion_score = completion
        density_score = density / max_density
        consistency_score = consistency / max_consistency
        
        # Weighted total score
        total_score = (
            minutes_score * 0.25 +
            sessions_score * 0.30 +
            completion_score * 0.25 +
            density_score * 0.15 +
            consistency_score * 0.05
        )
        
        return {
            'total': total_score,
            'minutes': minutes_score,
            'sessions': sessions_score,
            'completion': completion_score,
            'density': density_score,
            'consistency': consistency_score
        }
    
    def generate_fun_fact(self, song_key, basic_stats, session_starters, 
                         completion_rates, density_details, consistency_details):
        """Generate a fun insight about why this song ranked where it did"""
        facts = []
        
        # Session starter insights
        sessions = session_starters.get(song_key, 0)
        if sessions > 10:
            facts.append(f"💎 You chose this to START {sessions} listening sessions!")
        elif sessions > 5:
            facts.append(f"✨ Started {sessions} sessions with this one")
        
        # Completion rate insights
        comp_stats = completion_rates.get(song_key, {})
        comp_rate = comp_stats.get('completion_rate', 0)
        skipped = comp_stats.get('skipped_plays', 0)
        
        if comp_rate >= 0.95:
            facts.append(f"🎯 Almost never skipped ({comp_rate:.0%} completion)")
        elif comp_rate < 0.5 and skipped > 5:
            facts.append(f"⏭️ Skipped {skipped} times - maybe got sick of it?")
        
        # Density insights
        density = density_details.get(song_key, {})
        max_day = density.get('max_in_one_day', 0)
        multi_days = density.get('multi_listen_days', 0)
        
        if max_day >= 5:
            facts.append(f"🔥 Listened {max_day} times in one day - OBSESSED!")
        elif multi_days >= 10:
            facts.append(f"🔁 Had {multi_days} days with multiple listens")
        
        # Consistency insights
        consistency = consistency_details.get(song_key, {})
        months = consistency.get('months', 0)
        
        if months == 1:
            peak_month, peak_count = self.find_peak_month(song_key)
            if peak_month:
                facts.append(f"📅 Only in {peak_month} - a monthly obsession!")
        elif months >= 3:
            facts.append(f"📆 Consistent across {months} months")
        
        # Peak month
        peak_month, peak_count = self.find_peak_month(song_key)
        plays = basic_stats[song_key]['play_count']
        if peak_count > plays * 0.5:
            facts.append(f"📊 {peak_count}/{plays} plays were in {peak_month}")
        
        return " | ".join(facts) if facts else "Solid overall performance"
    
    def generate_reports(self):
        """Generate all rankings"""
        print("\n" + "="*80)
        print("ANALYZING YOUR SPOTIFY DATA...")
        print("="*80)
        
        # Calculate all metrics
        basic_stats = self.analyze_basic_metrics()
        session_starters = self.detect_session_starters()
        completion_rates = self.calculate_completion_rates()
        density_scores, density_details = self.calculate_listening_density()
        consistency_scores, consistency_details = self.calculate_consistency()
        
        # Filter out songs with very few plays (< 3)
        filtered_songs = {k: v for k, v in basic_stats.items() if v['play_count'] >= 3}
        
        print(f"\nAnalyzing {len(filtered_songs)} songs (with 3+ plays)")
        
        # Calculate weighted scores
        weighted_scores = {}
        score_components = {}
        for song_key in filtered_songs:
            scores = self.calculate_weighted_score(
                song_key, basic_stats, session_starters, 
                completion_rates, density_scores, consistency_scores
            )
            weighted_scores[song_key] = scores['total']
            score_components[song_key] = scores
        
        # Generate rankings
        self.print_ranking("TOP 10 BY PLAY COUNT", filtered_songs, 
                          lambda k: basic_stats[k]['play_count'], 
                          basic_stats, 10,
                          session_starters=session_starters,
                          completion_rates=completion_rates,
                          density_details=density_details,
                          consistency_details=consistency_details)
        
        self.print_ranking("TOP 10 BY MINUTES LISTENED", filtered_songs,
                          lambda k: basic_stats[k]['total_ms'] / 60000,
                          basic_stats, 10,
                          session_starters=session_starters,
                          completion_rates=completion_rates,
                          density_details=density_details,
                          consistency_details=consistency_details)
        
        self.print_ranking("🏆 TOP 10 WEIGHTED 'TRUE FAVORITES' 🏆", filtered_songs,
                          lambda k: weighted_scores[k],
                          basic_stats, 10,
                          score_components=score_components,
                          session_starters=session_starters,
                          completion_rates=completion_rates,
                          density_details=density_details,
                          consistency_details=consistency_details,
                          show_detailed_scores=True)
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE!")
        print("="*80)
    
    def print_ranking(self, title, songs, sort_key, basic_stats, limit=10, 
                     score_components=None, session_starters=None, 
                     completion_rates=None, density_details=None, 
                     consistency_details=None, show_detailed_scores=False):
        """Print a ranking table with detailed insights"""
        print(f"\n{title}")
        print("-" * 80)
        
        sorted_songs = sorted(songs.keys(), key=sort_key, reverse=True)[:limit]
        
        for i, song_key in enumerate(sorted_songs, 1):
            stats = basic_stats[song_key]
            track = stats['track_name']
            artist = stats['artist_name']
            plays = stats['play_count']
            minutes = stats['total_ms'] / 60000
            
            # Main info line
            print(f"\n{i:2d}. {track[:45]:<45} - {artist[:25]:<25}")
            print(f"    ♫ {plays:3d} plays | {minutes:6.1f} min", end="")
            
            # Add session starters and completion rate
            if session_starters:
                sessions = session_starters.get(song_key, 0)
                print(f" | ▶ {sessions} sessions", end="")
            
            if completion_rates:
                comp_rate = completion_rates.get(song_key, {}).get('completion_rate', 0)
                print(f" | ✓ {comp_rate:.0%} completed", end="")
            
            print()  # New line
            
            # Show detailed score breakdown for weighted ranking
            if show_detailed_scores and score_components:
                components = score_components[song_key]
                print(f"    📊 Score: {components['total']:.3f} = ", end="")
                print(f"Minutes({components['minutes']:.2f}×0.25) + ", end="")
                print(f"Sessions({components['sessions']:.2f}×0.30) + ", end="")
                print(f"Completion({components['completion']:.2f}×0.25) + ", end="")
                print(f"Density({components['density']:.2f}×0.15) + ", end="")
                print(f"Consistency({components['consistency']:.2f}×0.05)")
            
            # Fun fact
            if session_starters and completion_rates and density_details and consistency_details:
                fun_fact = self.generate_fun_fact(
                    song_key, basic_stats, session_starters, 
                    completion_rates, density_details, consistency_details
                )
                print(f"    💡 {fun_fact}")

# Usage
if __name__ == "__main__":
    # Change this to your data folder path
    DATA_FOLDER = "./spotify_data"  # Put your StreamingHistory*.json files here
    
    analyzer = SpotifyAnalyzer(DATA_FOLDER)
    analyzer.load_streaming_history()
    analyzer.generate_reports()