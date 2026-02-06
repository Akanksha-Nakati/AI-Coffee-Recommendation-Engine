# local_scrapers/scrape_reddit.py

"""
Reddit Coffee Discussion Scraper
Collects coffee recommendations and discussions from multiple subreddits
Target: 500-1000 posts with comments
"""

import praw
from dotenv import load_dotenv
import os
import json
from datetime import datetime
from typing import List, Dict
from tqdm import tqdm
import time

# Load environment variables
load_dotenv()


class RedditCoffeeScraper:
    """Scraper for coffee-related Reddit discussions"""
    
    def __init__(self):
        """Initialize Reddit API client"""
        
        print("🔧 Initializing Reddit scraper...")
        
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        # Verify connection
        print(f"✅ Connected as: {self.reddit.read_only}")
        
        self.discussions = []
        
        # Target subreddits
        self.subreddits = [
            'Coffee',           # General coffee enthusiasts
            'starbucks',        # Starbucks specific
            'cafe',             # Café culture
            'espresso',         # Espresso lovers
            'barista',          # Barista community
            'CoffeeGoneWild',   # Coffee photos and recommendations
        ]
        
        # Search queries for recommendations
        self.search_queries = [
            'recommend drink',
            'best order',
            'favorite customization',
            'coffee suggestion',
            'what should I get',
            'drink recommendation',
            'favorite drink',
            'best coffee',
            'cold brew recommendation',
            'latte suggestion',
            'iced coffee order',
            'sweet drink recommendation',
        ]
    
    def scrape_subreddit(self, subreddit_name: str, limit: int = 200) -> List[Dict]:
        """
        Scrape posts from a specific subreddit
        
        Args:
            subreddit_name: Name of subreddit
            limit: Maximum posts to fetch
        
        Returns:
            List of discussion dictionaries
        """
        
        print(f"\n☕ Scraping r/{subreddit_name}...")
        
        subreddit = self.reddit.subreddit(subreddit_name)
        discussions = []
        
        try:
            # Method 1: Search for recommendation-related posts
            for query in self.search_queries[:3]:  # Top 3 queries per subreddit
                print(f"  🔍 Searching: '{query}'")
                
                for post in subreddit.search(query, time_filter='year', limit=50):
                    
                    # Filter quality posts (minimum engagement)
                    if post.score < 5 or post.num_comments < 3:
                        continue
                    
                    discussion_data = self.extract_post_data(post, subreddit_name)
                    discussions.append(discussion_data)
                
                time.sleep(1)  # Rate limiting
            
            # Method 2: Get top posts from the past year
            print(f"  ⭐ Fetching top posts...")
            for post in subreddit.top(time_filter='year', limit=100):
                
                # Only get posts about recommendations/orders
                if any(keyword in post.title.lower() for keyword in 
                      ['recommend', 'favorite', 'best', 'order', 'suggestion', 'drink']):
                    
                    if post.score >= 10:  # Higher threshold for top posts
                        discussion_data = self.extract_post_data(post, subreddit_name)
                        discussions.append(discussion_data)
            
            # Remove duplicates based on post ID
            unique_discussions = {d['post_id']: d for d in discussions}.values()
            discussions = list(unique_discussions)
            
            print(f"  ✅ Collected {len(discussions)} posts from r/{subreddit_name}")
            
        except Exception as e:
            print(f"  ❌ Error scraping r/{subreddit_name}: {e}")
        
        return discussions
    
    def extract_post_data(self, post, subreddit_name: str) -> Dict:
        """
        Extract data from a Reddit post
        
        Args:
            post: PRAW Submission object
            subreddit_name: Name of subreddit
        
        Returns:
            Dictionary with post data
        """
        
        # Extract top comments
        post.comments.replace_more(limit=0)  # Remove "load more comments"
        top_comments = []
        
        for comment in list(post.comments)[:10]:  # Top 10 comments
            if hasattr(comment, 'body') and comment.score >= 2:
                top_comments.append({
                    'body': comment.body,
                    'score': comment.score,
                    'created_utc': datetime.fromtimestamp(comment.created_utc).isoformat()
                })
        
        # Build discussion data
        discussion = {
            'post_id': post.id,
            'title': post.title,
            'text': post.selftext,
            'author': str(post.author) if post.author else '[deleted]',
            'score': post.score,
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
            'url': f"https://reddit.com{post.permalink}",
            'subreddit': subreddit_name,
            'top_comments': top_comments,
            'flair': post.link_flair_text if post.link_flair_text else None,
            'scraped_date': datetime.now().isoformat()
        }
        
        # Extract mentioned drinks/keywords
        discussion['mentioned_drinks'] = self.extract_drink_mentions(
            post.title + ' ' + post.selftext + ' ' + 
            ' '.join([c['body'] for c in top_comments])
        )
        
        return discussion
    
    def extract_drink_mentions(self, text: str) -> List[str]:
        """
        Extract coffee drink mentions from text
        
        Args:
            text: Text to analyze
        
        Returns:
            List of mentioned drinks
        """
        
        drink_keywords = [
            'latte', 'cappuccino', 'americano', 'espresso', 'mocha',
            'macchiato', 'cortado', 'flat white', 'cold brew', 'iced coffee',
            'frappuccino', 'frappe', 'nitro', 'pour over', 'drip coffee',
            'caramel', 'vanilla', 'hazelnut', 'pumpkin spice', 'peppermint',
            'blonde', 'dark roast', 'medium roast', 'decaf'
        ]
        
        text_lower = text.lower()
        mentions = []
        
        for keyword in drink_keywords:
            if keyword in text_lower:
                mentions.append(keyword)
        
        return list(set(mentions))  # Remove duplicates
    
    def scrape_all_subreddits(self) -> List[Dict]:
        """Scrape all target subreddits"""
        
        print("\n🚀 Starting comprehensive Reddit scraping...")
        print(f"📋 Target subreddits: {', '.join(self.subreddits)}")
        
        all_discussions = []
        
        for subreddit_name in self.subreddits:
            try:
                discussions = self.scrape_subreddit(subreddit_name)
                all_discussions.extend(discussions)
                print(f"  Running total: {len(all_discussions)} discussions")
                
                # Be polite - wait between subreddits
                time.sleep(2)
                
            except Exception as e:
                print(f"  ⚠️ Failed to scrape r/{subreddit_name}: {e}")
                continue
        
        # Remove duplicates across subreddits
        unique_discussions = {d['post_id']: d for d in all_discussions}.values()
        self.discussions = list(unique_discussions)
        
        print(f"\n🎉 Scraping complete! Total unique discussions: {len(self.discussions)}")
        return self.discussions
    
    def get_statistics(self) -> Dict:
        """Generate statistics about scraped data"""
        
        if not self.discussions:
            return {}
        
        stats = {
            'total_posts': len(self.discussions),
            'total_comments': sum(len(d['top_comments']) for d in self.discussions),
            'avg_score': sum(d['score'] for d in self.discussions) / len(self.discussions),
            'subreddit_breakdown': {},
            'top_mentioned_drinks': {}
        }
        
        # Subreddit breakdown
        for discussion in self.discussions:
            sub = discussion['subreddit']
            stats['subreddit_breakdown'][sub] = stats['subreddit_breakdown'].get(sub, 0) + 1
        
        # Top mentioned drinks
        all_mentions = []
        for discussion in self.discussions:
            all_mentions.extend(discussion.get('mentioned_drinks', []))
        
        from collections import Counter
        drink_counts = Counter(all_mentions)
        stats['top_mentioned_drinks'] = dict(drink_counts.most_common(10))
        
        return stats


def save_to_json(discussions: List[Dict]) -> str:
    """Save Reddit discussions to JSON file"""
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    output_dir = 'data/raw/reddit'
    
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f'{output_dir}/coffee_discussions_{date_str}.json'
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(discussions, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Saved {len(discussions)} discussions to {filename}")
    return filename


def print_statistics(stats: Dict):
    """Print scraping statistics"""
    
    print("\n📊 Scraping Statistics:")
    print(f"  Total posts: {stats['total_posts']}")
    print(f"  Total comments collected: {stats['total_comments']}")
    print(f"  Average post score: {stats['avg_score']:.1f}")
    
    print("\n  Posts per subreddit:")
    for sub, count in stats['subreddit_breakdown'].items():
        print(f"    r/{sub}: {count}")
    
    print("\n  Top mentioned drinks:")
    for drink, count in list(stats['top_mentioned_drinks'].items())[:5]:
        print(f"    {drink}: {count} mentions")


def main():
    """Main execution"""
    
    # Check for credentials
    if not os.getenv('REDDIT_CLIENT_ID'):
        print("❌ Error: Reddit credentials not found!")
        print("Please set up your .env file with REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET")
        return
    
    scraper = RedditCoffeeScraper()
    
    # Scrape all subreddits
    discussions = scraper.scrape_all_subreddits()
    
    if discussions:
        # Save data
        save_to_json(discussions)
        
        # Print statistics
        stats = scraper.get_statistics()
        print_statistics(stats)
        
        # Show sample discussion
        print("\n📄 Sample Discussion:")
        sample = discussions[0]
        print(f"  Title: {sample['title']}")
        print(f"  Subreddit: r/{sample['subreddit']}")
        print(f"  Score: {sample['score']} | Comments: {sample['num_comments']}")
        print(f"  Mentioned drinks: {', '.join(sample['mentioned_drinks'][:5])}")
        
    else:
        print("⚠️ No discussions were scraped!")
    
    print("\n✨ Done!")


if __name__ == "__main__":
    main()
