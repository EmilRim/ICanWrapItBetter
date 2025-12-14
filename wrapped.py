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
            'completion_rate': 0.0
        })
        
        for stream in self.streams:
            key = self.get_song_key(stream)
            ms_played = stream.get('msPlayed', 0)
            
            # Try to get track duration from the stream data
            # Spotify sometimes includes duration, but not always
            # We'll estimate: if ms_played is close to typical song length ranges
            song_completions[key]['total_plays'] += 1
            
            # Consider it completed if played for more than 90 seconds (most songs are longer)
            # and seems like a full play (we'll refine this with actual data)
            if ms_played > 90000:  # 90 seconds
                song_completions[key]['completed_plays'] += 1
        
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
        for song_key, dates in daily_listens.items():
            # Count days with 2+ listens (shows obsession)
            multi_listen_days = sum(1 for count in dates.values() if count >= 2)
            density_scores[song_key] = multi_listen_days
        
        return density_scores
    
    def calculate_consistency(self):
        """Calculate how many different weeks/months song appeared in"""
        song_weeks = defaultdict(set)
        
        for stream in self.streams:
            key = self.get_song_key(stream)
            date = datetime.strptime(stream['endTime'], '%Y-%m-%d %H:%M')
            # ISO week number
            week = f"{date.year}-W{date.isocalendar()[1]}"
            song_weeks[key].add(week)
        
        consistency_scores = {k: len(v) for k, v in song_weeks.items()}
        return consistency_scores
    
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
        
        # Weighted score
        score = (
            (minutes / max_minutes) * 0.25 +  # 25% total minutes
            (sessions / max_sessions) * 0.30 +  # 30% session starters
            completion * 0.25 +  # 25% completion rate
            (density / max_density) * 0.15 +  # 15% listening density
            (consistency / max_consistency) * 0.05  # 5% consistency
        )
        
        return score
    
    def generate_reports(self):
        """Generate all rankings"""
        print("\n" + "="*60)
        print("ANALYZING YOUR SPOTIFY DATA...")
        print("="*60)
        
        # Calculate all metrics
        basic_stats = self.analyze_basic_metrics()
        session_starters = self.detect_session_starters()
        completion_rates = self.calculate_completion_rates()
        density_scores = self.calculate_listening_density()
        consistency_scores = self.calculate_consistency()
        
        # Filter out songs with very few plays (< 3)
        filtered_songs = {k: v for k, v in basic_stats.items() if v['play_count'] >= 3}
        
        print(f"\nAnalyzing {len(filtered_songs)} songs (with 3+ plays)")
        
        # Calculate weighted scores
        weighted_scores = {}
        for song_key in filtered_songs:
            weighted_scores[song_key] = self.calculate_weighted_score(
                song_key, basic_stats, session_starters, 
                completion_rates, density_scores, consistency_scores
            )
        
        # Generate rankings
        self.print_ranking("TOP 10 BY PLAY COUNT", filtered_songs, 
                          lambda k: basic_stats[k]['play_count'], 
                          basic_stats, 10)
        
        self.print_ranking("TOP 10 BY MINUTES LISTENED", filtered_songs,
                          lambda k: basic_stats[k]['total_ms'] / 60000,
                          basic_stats, 10)
        
        self.print_ranking("TOP 10 SESSION STARTERS", 
                          {k: v for k, v in filtered_songs.items() if k in session_starters},
                          lambda k: session_starters[k],
                          basic_stats, 10)
        
        self.print_ranking("TOP 10 LEAST SKIPPED (COMPLETION RATE)", filtered_songs,
                          lambda k: completion_rates.get(k, {}).get('completion_rate', 0),
                          basic_stats, 10,
                          extra_info=lambda k: f"Completion: {completion_rates.get(k, {}).get('completion_rate', 0):.1%}")
        
        self.print_ranking("🏆 TOP 10 WEIGHTED 'TRUE FAVORITES' 🏆", filtered_songs,
                          lambda k: weighted_scores[k],
                          basic_stats, 10,
                          extra_info=lambda k: f"Score: {weighted_scores[k]:.3f}")
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE!")
        print("="*60)
    
    def print_ranking(self, title, songs, sort_key, basic_stats, limit=10, extra_info=None):
        """Print a ranking table"""
        print(f"\n{title}")
        print("-" * 60)
        
        sorted_songs = sorted(songs.keys(), key=sort_key, reverse=True)[:limit]
        
        for i, song_key in enumerate(sorted_songs, 1):
            stats = basic_stats[song_key]
            track = stats['track_name']
            artist = stats['artist_name']
            plays = stats['play_count']
            minutes = stats['total_ms'] / 60000
            
            info = f"{i:2d}. {track[:40]:<40} - {artist[:25]:<25}"
            info += f"\n    Plays: {plays:3d} | Minutes: {minutes:6.1f}"
            
            if extra_info:
                info += f" | {extra_info(song_key)}"
            
            print(info)

# Usage
if __name__ == "__main__":
    # Change this to your data folder path
    DATA_FOLDER = "./spotify_data"  # Put your StreamingHistory*.json files here
    
    analyzer = SpotifyAnalyzer(DATA_FOLDER)
    analyzer.load_streaming_history()
    analyzer.generate_reports()