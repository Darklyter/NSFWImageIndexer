import os, json, time, re, argparse, exiftool, threading, queue, calendar, io, uuid, requests, shutil, zipfile
from pathlib import Path
from json_repair import repair_json as rj
from datetime import timedelta, datetime
from .image_processor import ImageProcessor
from .llmii_utils import first_json, de_pluralize, AND_EXCEPTIONS
from . import llmii_db
    
def split_on_internal_capital(word):
    """ Split a word if it contains a capital letter after the 4th position.
        Returns the original word if no split is needed, or the split 
        version if a capital is found.
        
        Examples:
            BlueSky -> Blue Sky
            microService -> micro Service
    """
    if len(word) <= 4:
        return word
    
    for i in range(4, len(word)):
        if word[i].isupper():
            return word[:i] + " " + word[i:]
            
    return word

def normalize_keyword(keyword, banned_words, config=None):
    """ Normalizes keywords according to specific rules:
        - Splits unhyphenated compound words on internal capitals
        - Max words determined by config (default 2) unless middle word is 'and'/'or' (then +1)
        - If split_and_entries enabled, remove and/or unless in exceptions list
        - Hyphens between alphanumeric chars count as two words
        - Cannot start with 3+ digits if no_digits_start is enabled
        - Each word must be 2+ chars if min_word_length enabled (unless it is x or u)
        - Removes all non-alphanumeric except spaces and valid hyphens
        - Checks against banned words if ban_prompt_words enabled
        - Makes singular if depluralize_keywords enabled
        - Returns lowercase result
    """   
    if config is None:
        class DefaultConfig:
            def __init__(self):
                self.normalize_keywords = True
                self.depluralize_keywords = True
                self.limit_word_count = True
                self.max_words_per_keyword = 2
                self.split_and_entries = True
                self.ban_prompt_words = True
                self.no_digits_start = True
                self.min_word_length = True
                self.latin_only = True
        
        config = DefaultConfig()
    
    if not config.normalize_keywords:
        return keyword.strip()
    
    if not isinstance(keyword, str):
        keyword = str(keyword)
    
    # Handle internal capitalization before lowercase conversion
    words = keyword.strip().split()
    split_words = []
    
    for word in words:
        split_words.extend(split_on_internal_capital(word).split())
    
    keyword = " ".join(split_words)
    
    # Convert to lowercase after handling capitals
    keyword = keyword.lower().strip()
    
    # Remove non-Latin characters if latin_only is enabled
    if config.latin_only:
        keyword = re.sub(r'[^\x00-\x7F]', '', keyword)
    
    # Remove all non-alphanumeric chars except spaces and hyphens
    keyword = re.sub(r'[^\w\s-]', '', keyword)
    
    # Replace multiple spaces/hyphens with single space/hyphen
    keyword = re.sub(r'\s+', ' ', keyword)
    keyword = re.sub(r'-+', '-', keyword)
    keyword = re.sub(r'_', ' ', keyword)
    
    # Check for banned words if enabled
    if config.ban_prompt_words and keyword in banned_words:
        return None
    
    # For validation, we'll track both original tokens and split words
    tokens = keyword.split()
    words = []
    
    # Validate and collect words for length checking
    for token in tokens:    
        
        # Handle hyphenated words
        if '-' in token:
            
            # Check if hyphen is between alphanumeric chars
            if not re.match(r'^[\w]+-[\w]+$', token):
                return None
           
            # Add hyphenated parts to words list for validation
            parts = token.split('-')
            words.extend(parts)
        
        else:
            words.append(token)
    
    # Validate word count if limit_word_count is enabled
    if config.limit_word_count:
        max_words = config.max_words_per_keyword
        if len(words) > max_words + 1:
            return None
        
    # Handle and/or splitting if enabled
    if config.split_and_entries and len(words) == 3 and words[1] in ['and', 'or']:
        if ' '.join(words) in AND_EXCEPTIONS:
            pass
        else:
            # Remove and/or and make singular if depluralize_keywords is enabled
            if config.depluralize_keywords:
                tokens = [de_pluralize(words[0]), de_pluralize(words[2])]
            else:
                tokens = [words[0], words[2]]
    
    # Word validation
    for word in words:
        
        # Check minimum length if enabled
        if config.min_word_length:
            if len(word) < 2 and word not in ['x', 'u']:
                return None
        
    # Check if starts with 3+ digits if enabled
    if config.no_digits_start and words and re.match(r'^\d{3,}', words[0]):
        return None
    
    # Make words singular if depluralize_keywords is enabled
    if config.depluralize_keywords:
        # Make solo words singular
        if len(words) == 1:
            tokens = [de_pluralize(words[0])]
        # If two or more words make the last word singular
        elif len(tokens) > 1:
            tokens[-1] = de_pluralize(tokens[-1])
    
    # Return the original tokens (preserving hyphens)
    return ' '.join(tokens)
    
def clean_string(data):
    """ Makes sure the string is clean for addition
        to the metadata.
    """
        
    if isinstance(data, dict):
        data = json.dumps(data)
    
    # Remove <think> content
    if isinstance(data, str):
        # Remove matched pairs first
        data = re.sub(r'<think>.*?</think>', '', data, flags=re.DOTALL)
        # Remove any remaining orphaned opening tags
        data = re.sub(r'<think>', '', data)
        # Remove any remaining orphaned closing tags
        data = re.sub(r'</think>', '', data)
        
        # Normalize
        data = re.sub(r"\n", "", data)
        data = re.sub(r'["""]', '"', data)
        data = re.sub(r"\\{2}", "", data)
        last_period = data.rfind('.')
        
        if last_period != -1:
            data = data[:last_period+1]
    else:
        return ""
        
    return data
    

def markdown_list_to_dict(text):
    """ Searches a string for a markdown formatted
        list, and if one is found, converts it to
        a dict.
    """
    list_pattern = r"(?:^\s*[-*+]|\d+\.)\s*(.+)$"
    list_items = re.findall(list_pattern, text, re.MULTILINE)

    if list_items:
        return {"Keywords": list_items}
    else:
        return None
        
def clean_json(data):
    """ LLMs like to return all sorts of garbage.
        Even when asked to give a structured output
        they will wrap text around it explaining why
        they chose certain things. This function
        will pull basically anything useful and turn it
        into a dict.

        Handles various formats including:
        - Direct dicts
        - List-wrapped dicts: [{"Description": ...}]
        - String JSON with markdown wrappers
        - Malformed JSON requiring repair
    """
    if data is None:
        return None

    if isinstance(data, dict):
        return data

    # Handle list-wrapped dicts (sometimes APIs return [{"Description": ...}])
    if isinstance(data, list) and len(data) > 0:
        if isinstance(data[0], dict):
            return data[0]

    if isinstance(data, str):
        # Try direct JSON parsing first (works with JSON grammar)
        try:
            result = json.loads(data)
            # If result is a list with a dict, unwrap it
            if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
                return result[0]
            return result
        except:
            pass

        # Try to extract JSON markdown code
        pattern = r"```json\s*(.*?)\s*```"
        match = re.search(pattern, data, re.DOTALL)
        if match:
            data = match.group(1).strip()
            try:
                result = json.loads(data)
                if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
                    return result[0]
                return result
            except:
                pass

        # Fallback: Try with repair_json
        try:
            result = json.loads(rj(data))
            if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
                return result[0]
            return result
        except:
            pass

        # Fallback: first_json + repair_json
        try:
            result = json.loads(rj(first_json(data)))
            if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
                return result[0]
            return result
        except:
            pass

        # Nuclear option: wrap in brackets and repair
        try:
            result = json.loads(first_json(rj("{" + data + "}")))
            if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
                result = result[0]
            if result.get("Keywords"):
                return result
        except:
            pass
    
        # Strangelove option
        try:
            return markdown_list_to_dict(data)
        except:
            pass

    return None

def clean_tags(data):
    """ Extract and combine all Keywords entries from LLM output.
        When EOS token is banned, the model may generate multiple
        JSON objects with Keywords arrays. This function finds all
        of them and combines them into a single Keywords list.

        Returns a dict with a single Keywords key containing all found keywords.
    """
    all_keywords = []

    if data is None:
        return None

    if isinstance(data, dict):
        # Single dict - extract Keywords if present
        keywords = data.get("Keywords", [])
        if keywords:
            all_keywords.extend(keywords)
        return {"Keywords": all_keywords} if all_keywords else None

    if isinstance(data, list):
        # Raw string list - model returned keywords directly as an array
        if data and all(isinstance(item, str) for item in data):
            seen = set()
            deduped = [k for k in data if not (k.lower() in seen or seen.add(k.lower()))][:30]
            return {"Keywords": deduped}
        # List of dicts - extract Keywords from each
        for item in data:
            if isinstance(item, dict):
                keywords = item.get("Keywords", [])
                if keywords:
                    all_keywords.extend(keywords)
        return {"Keywords": all_keywords} if all_keywords else None

    if isinstance(data, str):
        # Try to find all JSON objects in the string
        # First, try to parse as single JSON
        try:
            parsed = json.loads(data)
            return clean_tags(parsed)  # Recursively handle the parsed result
        except:
            pass

        # Try to extract from JSON markdown
        pattern = r"```json\s*(.*?)\s*```"
        match = re.search(pattern, data, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group(1))
                return clean_tags(parsed)
            except:
                pass

        # Try to find multiple JSON objects in the string
        # Look for all {"Keywords": [...]} patterns
        keywords_pattern = r'"Keywords"\s*:\s*\[(.*?)\]'
        matches = re.findall(keywords_pattern, data, re.DOTALL)

        for match in matches:
            # Try to parse the array content
            try:
                # Reconstruct the JSON array and parse it
                array_str = '[' + match + ']'
                keywords = json.loads(array_str)
                if keywords:
                    all_keywords.extend(keywords)
            except:
                # If parsing fails, try with repair_json
                try:
                    array_str = '[' + match + ']'
                    keywords = json.loads(rj(array_str))
                    if keywords:
                        all_keywords.extend(keywords)
                except:
                    pass

        if all_keywords:
            return {"Keywords": all_keywords}

        # Last resort: try repair_json on the whole string
        try:
            parsed = json.loads(rj(data))
            return clean_tags(parsed)
        except:
            pass

    return None


class TagMatcher:
    """Matches LLM-generated keywords to canonical tags from tags_export.json.

    Looks for tags_export.json in the project root (two levels above this file).
    Each entry in the file has a "tag" (canonical) and an "alias" (variant spelling).
    Multiple aliases can map to the same tag.

    Matching order per keyword:
      1. Exact match (case-insensitive, stripped) against all aliases
      2. Fuzzy token-set match via rapidfuzz (threshold 70/100)

    Unmatched keywords are appended to unmatched_keywords.log in the same directory.
    When tags_export.json is absent the matcher is disabled and the caller falls back
    to normal normalize_keyword behaviour.
    """

    FUZZY_THRESHOLD = 85  # class-level default; instances use self.fuzzy_threshold

    @staticmethod
    def _normalize(text: str) -> str:
        """Normalize a tag/alias for comparison.

        Replaces hyphens and underscores with spaces, strips all remaining
        punctuation, lowercases, and collapses whitespace.  This ensures
        'blue-colored hair' and 'blue colored hair' compare as equal.
        """
        text = re.sub(r'[-_]', ' ', text.lower())
        text = re.sub(r"[^a-z0-9\s]", '', text)
        return ' '.join(text.split())

    def __init__(self, tags_file_path: Path, name: str = None,
                 matched_log_path: Path = None, unmatched_log_path: Path = None,
                 fuzzy_threshold: int = None):
        self.enabled = False
        self.fuzzy_threshold = fuzzy_threshold if fuzzy_threshold is not None else self.FUZZY_THRESHOLD
        self.name = name or tags_file_path.stem
        self._exact: dict = {}      # normalized alias -> canonical tag
        self._aliases: list = []    # all normalized aliases for fuzzy search
        self._alias_tags: list = [] # parallel list: _aliases[i] -> canonical tag
        self._log_path: Path = unmatched_log_path or (tags_file_path.parent / "unmatched_keywords.log")
        self._matched_log_path: Path = matched_log_path or (tags_file_path.parent / "matched_keywords.log")
        self.suppress_file_logging = False  # set True when DB captures logs instead
        self._load(tags_file_path)

    def _load(self, path: Path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            print("TagMatcher: tags_export.json not found — tag matching disabled")
            return
        except Exception as e:
            print(f"TagMatcher: failed to load {path}: {e}")
            return

        for entry in data:
            # Support both {"tag": ..., "alias": ...} and {"Tag": ..., "Alias": ...}
            tag = (entry.get("tag") or entry.get("Tag") or "").strip()
            alias = (entry.get("alias") or entry.get("Alias") or "").strip()
            if not tag or not alias:
                continue
            key = TagMatcher._normalize(alias)
            if key not in self._exact:
                self._exact[key] = tag
            self._aliases.append(key)
            self._alias_tags.append(tag)

            # Also register the canonical tag name for exact and fuzzy matching
            # so that a keyword like "socks" finds tag "Socks" even when no
            # alias is exactly "socks". Only set if not already claimed by an
            # alias entry.
            tag_key = TagMatcher._normalize(tag)
            if tag_key not in self._exact:
                self._exact[tag_key] = tag
                self._aliases.append(tag_key)
                self._alias_tags.append(tag)

        unique_tags = len(set(self._alias_tags))
        if self._aliases:
            print(f"TagMatcher: loaded {len(self._aliases)} aliases across {unique_tags} unique tags")
            self.enabled = True
        else:
            print(f"TagMatcher: {path.name} loaded but contained no usable tag/alias entries — tag matching disabled")

    def load_from_db(self, conn):
        """Replace the JSON-loaded tag vocabulary with data from the database.

        Queries tags + tag_aliases from the ai_captioning schema and rebuilds
        the exact-lookup dict and fuzzy alias lists using the same logic as
        _load().  Safe to call after __init__ — any previously loaded data is
        discarded first.
        """
        self._exact = {}
        self._aliases = []
        self._alias_tags = []
        self.enabled = False

        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT t.tag, a.alias '
                    'FROM tag_aliases a JOIN tags t ON t.id = a.tag_id'
                )
                rows = cur.fetchall()
        except Exception as e:
            print(f"TagMatcher: failed to load tags from database: {e}")
            return

        for tag, alias in rows:
            key = TagMatcher._normalize(alias)
            if key not in self._exact:
                self._exact[key] = tag
            self._aliases.append(key)
            self._alias_tags.append(tag)

            # Auto-register the canonical tag name exactly as _load() does
            tag_key = TagMatcher._normalize(tag)
            if tag_key not in self._exact:
                self._exact[tag_key] = tag
                self._aliases.append(tag_key)
                self._alias_tags.append(tag)

        unique_tags = len(set(self._alias_tags))
        if self._aliases:
            print(f"TagMatcher: loaded {len(self._aliases)} aliases across {unique_tags} unique tags from database")
            self.enabled = True
        else:
            print("TagMatcher: database returned no tag/alias entries — tag matching disabled")

    # Gendered words used to detect keyword/alias gender conflicts.
    # Matched as whole tokens only (not substrings) to avoid false matches
    # like "human" triggering "man" or "female" inside "maleficent".
    _FEMININE = frozenset({"woman", "women", "girl", "girls", "lady", "ladies",
                           "female", "females", "gal", "gals"})
    _MASCULINE = frozenset({"man", "men", "boy", "boys", "male", "males",
                            "guy", "guys"})

    @staticmethod
    def _gender(text: str):
        """Return 'female', 'male', or None based on whole-word gendered tokens."""
        tokens = set(re.findall(r'\b[a-z]+\b', text.lower()))
        if tokens & TagMatcher._FEMININE:
            return 'female'
        if tokens & TagMatcher._MASCULINE:
            return 'male'
        return None

    @staticmethod
    def _gender_conflicts(keyword: str, alias: str) -> bool:
        """Return True if keyword and alias carry opposing gendered terms."""
        kg = TagMatcher._gender(keyword)
        ag = TagMatcher._gender(alias)
        return bool(kg and ag and kg != ag)

    def match(self, keyword: str):
        """Return (canonical_tag, matched_alias) for a keyword, or None if no match found.

        All candidates — exact and fuzzy — are ranked together by score so the
        globally strongest match wins. Exact matches score 100 and always beat
        fuzzy ones. Gender-conflicting candidates are skipped, allowing a
        lower-scoring but gender-correct runner-up to be returned.
        """
        if not self.enabled:
            return None

        k = TagMatcher._normalize(keyword)

        try:
            from rapidfuzz import process as rfprocess, fuzz as rffuzz
            results = rfprocess.extract(
                k,
                self._aliases,
                scorer=rffuzz.token_sort_ratio,
                score_cutoff=self.fuzzy_threshold,
                limit=25,
            )
            for alias_str, _score, idx in results:
                if not self._gender_conflicts(k, alias_str):
                    return (self._alias_tags[idx], alias_str)
        except ImportError:
            # rapidfuzz unavailable — fall back to exact-only lookup
            if k in self._exact and not self._gender_conflicts(k, k):
                return (self._exact[k], k)

        return None

    def log_unmatched(self, keyword: str):
        """Append an unmatched keyword to the log file (one per line).
        No-op when suppress_file_logging is True (e.g. DB mode captures it instead)."""
        if self.suppress_file_logging:
            return
        try:
            with open(self._log_path, 'a', encoding='utf-8') as f:
                f.write(keyword + '\n')
        except Exception:
            pass

    def log_matched(self, keyword: str, alias: str, tag: str):
        """Append a matched keyword entry to matched_keywords.log as a JSON line.
        No-op when suppress_file_logging is True (e.g. DB mode captures it instead)."""
        if self.suppress_file_logging:
            return
        try:
            entry = {"keyword": keyword, "alias": alias, "tag": tag, "source": self.name}
            with open(self._matched_log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        except Exception:
            pass


def _checkpoint_path():
    """Return the path to the session checkpoint file (project root)."""
    return Path(__file__).resolve().parent.parent / 'llmii_checkpoint.json'


class Config:
    def __init__(self):
        self.directory = None
        self.api_url = None
        self.api_password = None
        self.no_crawl = False
        self.no_backup = False
        self.dry_run = False
        self.update_keywords = False
        self.reprocess_failed = False
        self.reprocess_all = False
        self.reprocess_orphans = True
        self.reprocess_sparse = False       # reprocess images with fewer than N keywords
        self.reprocess_sparse_min = 5       # threshold for "sparse"
        self.resume_session = False         # skip already-processed files on startup
        self.text_completion = False
        self.gen_count = 1500
        self.res_limit = 1280
        self.detailed_caption = True
        self.short_caption = False
        self.skip_verify = False
        self.quick_fail = False
        self.no_caption = False
        self.update_caption = False
        self.use_sidecar = False
        self.sidecar_dir = ""  # empty = save alongside images; path = save all sidecars there with embedded filename
        self.normalize_keywords = True
        self.depluralize_keywords = True
        self.limit_word_count = True
        self.max_words_per_keyword = 3
        self.split_and_entries = True
        self.ban_prompt_words = True
        self.no_digits_start = True  
        self.min_word_length = True
        self.latin_only = True
        self.tag_blacklist = []  # list of lowercase substrings; matched tags containing any are skipped
        self.tag_fuzzy_threshold = TagMatcher.FUZZY_THRESHOLD
        self.caption_instruction = "Describe the image in detail. Be specific and include all visible details including nudity, body parts, poses, expressions, clothing or lack thereof, and any explicit sexual content visible."
        self.system_instruction = "You are an image cataloguing assistant. Describe images accurately and completely, including explicit or adult content. Do not omit, censor, or euphemize any visible content."
        self.keyword_instruction = ""
        self.tag_instruction = (
            'Return a JSON object with key "Keywords" containing an array of concise tags '
            'that describe the image. Include tags from ALL applicable categories:\n'
            '- Nudity/clothing: nude, topless, bottomless, clothed, partially clothed, specific garments\n'
            '- Body/anatomy: visible body parts (breasts, nipples, vulva, vagina, labia, clitoris, penis, anus, buttocks, pubic area)\n'
            '- Pubic hair: shaved, bare, trimmed, natural, landing strip, full bush\n'
            '- Pose/position: standing, sitting, kneeling, lying down, bending over, specific poses\n'
            '- Sexual acts or states: aroused, spread, penetration, masturbation, oral sex, etc. if visible\n'
            '- Physical attributes: hair color/style, eye color, body type, skin tone, tattoos, piercings\n'
            '- Setting/environment: indoor, outdoor, bedroom, bathroom, studio, nature, etc.\n'
            '- Mood/expression: smiling, serious, seductive, playful, etc.\n'
            'Return only the JSON object: {"Keywords": []}'
        ) 

        # Sampler settings
        self.temperature = 0.1
        self.top_p = 0.9
        self.rep_pen = 1.05
        self.top_k = 50
        self.min_p = 0.05
        self.use_default_badwordsids = False
        self.use_json_grammar = True
        self.skip_folders = []
        self.rename_invalid = False
        self.preserve_date = False
        self.fix_extension = False
        #self.write_unsafe = False

        self.instruction = """Return a JSON object containing a Description for the image and a list of Keywords.

Write the Description using the active voice.

Generate 5 to 10 Keywords. Each Keyword is an item in a list and will be composed of a maximum of two words.

For both Description and Keywords, make sure to include:

 - Themes, concepts
 - Items, animals, objects
 - Structures, landmarks, setting
 - Foreground and background elements
 - Notable colors, textures, styles
 - Actions, activities

If humans are present, include:
 - Physical appearance
 - Gender
 - Clothing
 - Age range
 - Visibly apparent ancestry
 - Occupation/role
 - Relationships between individuals
 - Emotions, expressions, body language

Use ENGLISH only. Generate ONLY a JSON object with the keys Description and Keywords as follows {"Description": str, "Keywords": []}"""
        

        self.image_extensions = {
        "JPEG": [
            ".jpg",
            ".jpeg",
            ".jpe",
            ".jif",
            ".jfif",
            ".jfi",
            ".jp2",
            ".j2k",
            ".jpf",
            ".jpx",
            ".jpm",
            ".mj2",
        ],
        "PNG": [".png"],
        "GIF": [".gif"],
        "TIFF": [".tiff", ".tif"],
        "WEBP": [".webp"],
        "HEIF": [".heif", ".heic"],
        "RAW": [
            ".raw",  # Generic RAW
            ".arw",  # Sony
            ".cr2",  # Canon
            ".cr3",  # Canon (newer format)
            ".dng",  # Adobe Digital Negative
            ".nef",  # Nikon
            ".nrw",  # Nikon
            ".orf",  # Olympus
            ".pef",  # Pentax
            ".raf",  # Fujifilm
            ".rw2",  # Panasonic
            ".srw",  # Samsung
            ".x3f",  # Sigma
            ".erf",  # Epson
            ".kdc",  # Kodak
            ".rwl",  # Leica
        ],
        "ZIP": [".zip"],
        }
        self.image_extensions_filter = "jpg,jpeg,webp,zip"
        self.tags_file = "mastertags.json"

        # Temporary folder for zip extraction (cleared on every run)
        self.temp_folder = "temp"

        # Output mode and database connection settings
        self.output_mode = 'json'   # 'json' | 'db' | 'both'
        self.db_host     = 'localhost'
        self.db_port     = 5432
        self.db_user     = ''
        self.db_password = ''
        self.db_name     = ''

    @classmethod
    def from_args(cls):
        parser = argparse.ArgumentParser(description="Image Indexer")
        parser.add_argument("directory", help="Directory containing the files")
        parser.add_argument(
            "--api-url", default="http://localhost:5001", help="URL for the LLM API"
        )
        parser.add_argument(
            "--api-password", default="", help="Password for the LLM API"
        )
        parser.add_argument(
            "--no-crawl", action="store_true", help="Disable recursive indexing"
        )
        parser.add_argument(
            "--no-backup",
            action="store_true",
            help="Don't make a backup of files before writing",
        )
        parser.add_argument(
            "--dry-run", action="store_true", help="Don't write any files"
        )
        parser.add_argument(
            "--reprocess-all", action="store_true", help="Reprocess all files"
        )
        parser.add_argument(
            "--reprocess-failed", action="store_true", help="Reprocess failed files"
        )
        parser.add_argument(
            "--use-sidecar", action="store_true", help="Store generated data in an xmp sidecare instead of the image file"
        )
        parser.add_argument(
            "--reprocess-orphans", action="store_true", help="If a file has a UUID, determine its status"
        )
        parser.add_argument(
            "--update-keywords", action="store_true", help="Update existing keyword metadata"
        )
        parser.add_argument(
            "--gen-count", default=150, help="Number of tokens to generate"
        )
        parser.add_argument("--detailed-caption", action="store_true", help="Write a detailed caption along with keywords")
        parser.add_argument(
            "--skip-verify", action="store_true", help="Skip verifying file metadata validity before processing"
        )
        parser.add_argument("--update-caption", action="store_true", help="Add the generated caption to the existing description tag")
        parser.add_argument("--quick-fail", action="store_true", help="Mark failed after one try")
        parser.add_argument("--short-caption", action="store_true", help="Write a short caption along with keywords")
        parser.add_argument("--no-caption", action="store_true", help="Do not modify caption")
        parser.add_argument(
            "--normalize-keywords", action="store_true", help="Enable keyword normalization"
        )
        parser.add_argument("--res-limit", type=int, default=448, help="Limit the resolution of the image")
        parser.add_argument("--rename-invalid", type="store_true", help="Use rename invalid files so they don't get reprocessed")
        parser.add_argument("--preserve-date", type="store_true", help="Keep the original modified date, but will use a temp file when writing")
        args = parser.parse_args()

        config = cls()
        
        for key, value in vars(args).items():
            setattr(config, key, value)
        
        return config

class LLMProcessor:
    def __init__(self, config):
        self.api_url = config.api_url
        self.config = config
        self.instruction = config.instruction
        self.system_instruction = config.system_instruction
        self.caption_instruction = config.caption_instruction
        self.tag_instruction = config.tag_instruction
        self.requests = requests
        self.api_password = config.api_password
        self.max_tokens = config.gen_count
        self.temperature = config.temperature
        self.top_p = config.top_p
        self.rep_pen = config.rep_pen
        self.top_k = config.top_k
        self.min_p = config.min_p
        self.use_default_badwordsids = config.use_default_badwordsids
        self.use_json_grammar = config.use_json_grammar

    def describe_content(self, task="", processed_image=None, description=None):
        if task != "keywords_from_text" and not processed_image:
            print("No image to describe.")
            return None

        # Determine instruction and whether to ban EOS token based on task
        if task == "caption":
            instruction = self.caption_instruction
            ban_eos = self.use_default_badwordsids

        elif task == "keywords":
            instruction = self.tag_instruction
            ban_eos = self.use_default_badwordsids

        elif task == "keywords_from_text":
            instruction = self.tag_instruction
            ban_eos = self.use_default_badwordsids

        elif task == "caption_and_keywords":
            instruction = self.instruction
            ban_eos = self.use_default_badwordsids

        else:
            print(f"invalid task: {task}")
            return None

        try:
            # keywords_from_text uses description text only — no image
            if task == "keywords_from_text":
                messages = [
                    {"role": "system", "content": self.system_instruction},
                    {
                        "role": "user",
                        "content": f"{instruction}\n\nImage description:\n{description}"
                    }
                ]
            else:
                messages = [
                    {"role": "system", "content": self.system_instruction},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": instruction},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{processed_image}"
                                }
                            }
                        ]
                    }
                ]

            payload = {
                "messages": messages,
                "max_tokens": self.max_tokens,
                "temperature": self.temperature,
                "top_p": self.top_p,
                "top_k": self.top_k,
                "min_p": self.min_p,
                "rep_pen": self.rep_pen,
                "use_default_badwordsids": ban_eos
            }

            # Add JSON schema if grammar is enabled and task requires structured output
            if self.use_json_grammar and task in ["caption_and_keywords", "keywords", "keywords_from_text"]:
                if task == "caption_and_keywords":
                    # Schema for both description and keywords
                    payload["response_format"] = {
                        "type": "json_object",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "Description": {
                                    "type": "string"
                                },
                                "Keywords": {
                                    "type": "array",
                                    "items": {
                                        "type": "string"
                                    }
                                }
                            },
                            "required": ["Description", "Keywords"]
                        }
                    }
                elif task == "keywords":
                    # Schema for keywords only
                    payload["response_format"] = {
                        "type": "json_object",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "Keywords": {
                                    "type": "array",
                                    "items": {
                                        "type": "string"
                                    }
                                }
                            },
                            "required": ["Keywords"]
                        }
                    }

            endpoint = f"{self.api_url}/v1/chat/completions"
            headers = {
                "Content-Type": "application/json"
            }
            if self.api_password:
                headers["Authorization"] = f"Bearer {self.api_password}"

            response = self.requests.post(
                endpoint,
                json=payload,
                headers=headers,
                timeout=300,  # 5 minutes — generous for slow GPU inference
            )

            response.raise_for_status()
            response_json = response.json()

            if "choices" in response_json and len(response_json["choices"]) > 0:
                if "message" in response_json["choices"][0]:
                    content = response_json["choices"][0]["message"]["content"]
                else:
                    content = response_json["choices"][0].get("text", "")
                # Detect degenerate looping output (same word repeated >60% of tokens)
                words = content.split()
                if len(words) >= 10:
                    most_common = max(set(words), key=words.count)
                    if words.count(most_common) / len(words) > 0.6:
                        print(f"  Degenerate output ('{most_common}' repeated {words.count(most_common)}x) — will retry")
                        return None
                return content
            print(f"  Warning: API response missing expected data")
            return None
            
        except requests.exceptions.ConnectionError as e:
            print(f"API Connection Error: Cannot connect to {self.api_url}")
            print(f"  Make sure the LLM server is running and accessible")
            return None
        except requests.exceptions.Timeout as e:
            print(f"API Timeout Error: Request to {self.api_url} timed out")
            return None
        except requests.exceptions.HTTPError as e:
            print(f"API HTTP Error: {e.response.status_code} - {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"  Response: {e.response.text[:200]}")
            return None
        except Exception as e:
            print(f"API Error: {type(e).__name__} - {str(e)}")
            return None

class BackgroundIndexer(threading.Thread):
    def __init__(self, root_dir, metadata_queue, file_extensions, no_crawl=False, chunk_size=100, skip_folders=None, skip_paths=None):
        threading.Thread.__init__(self)
        self.root_dir = root_dir
        self.metadata_queue = metadata_queue
        self.file_extensions = file_extensions
        self.no_crawl = no_crawl
        self.skip_folders = skip_folders if skip_folders else []
        self.skip_paths = skip_paths if skip_paths else set()
        self.total_files_found = 0
        self.total_directories = 0   # set before indexing starts; used for progress bars
        self.indexing_complete = False
        self.chunk_size = chunk_size
        self.last_processed_dir = None

    def _should_skip_directory(self, directory):
        """Check if directory should be skipped based on skip_folders list"""
        if not self.skip_folders:
            return False

        # Normalize the directory path
        dir_normalized = os.path.normpath(directory)

        for skip_folder in self.skip_folders:
            skip_normalized = os.path.normpath(skip_folder)

            # Check if it's a full path match
            if dir_normalized == skip_normalized:
                return True

            # Check if it's a relative path from root_dir
            relative_skip = os.path.normpath(os.path.join(self.root_dir, skip_folder))
            if dir_normalized == relative_skip:
                return True

            # Check if the directory contains the skip folder in its path
            if skip_normalized in dir_normalized or os.path.basename(dir_normalized) == os.path.basename(skip_normalized):
                return True

        return False
            
    def run(self):
        if self.no_crawl:
            if not self._should_skip_directory(self.root_dir):
                self.total_directories = 1
                print(f"Indexing directory (no crawl): {self.root_dir}")
                self._index_directory(self.root_dir)
        else:
            # Get ordered list of directories to process
            directories = []
            for root, _, _ in os.walk(self.root_dir):
                dir_path = os.path.normpath(root)
                if not self._should_skip_directory(dir_path):
                    directories.append(dir_path)

            directories.sort()
            self.total_directories = len(directories)
            print(f"Found {len(directories)} director(ies) to index")

            for directory in directories:
                self._index_directory(directory)

        print(f"Indexing complete. Total files found: {self.total_files_found}")
        self.indexing_complete = True

    def _index_directory(self, directory):
        """Process directory in chunks"""
        directory = os.path.normpath(directory)
        file_batch = []
        
        try:
            for filename in sorted(os.listdir(directory)):
                file_path = os.path.normpath(os.path.join(directory, filename))
                
                # Skip if not a valid file type
                if not any(file_path.lower().endswith(ext) for ext in self.file_extensions):
                    continue

                # Skip files already processed (resume-session fast-path)
                if self.skip_paths and file_path in self.skip_paths:
                    continue
                        
                try:
                    # Check for 0 byte files
                    size = os.path.getsize(file_path)
                    if size > 0:
                        file_batch.append(file_path)
                        
                        # When we reach chunk size, send batch to queue
                        if len(file_batch) >= self.chunk_size:
                            self.total_files_found += len(file_batch)
                            self.metadata_queue.put((directory, file_batch))
                            file_batch = []
                            
                except (FileNotFoundError, PermissionError, OSError):
                    continue
                    
            # Don't forget the last batch
            if file_batch:
                self.total_files_found += len(file_batch)
                self.metadata_queue.put((directory, file_batch))
                
        except (PermissionError, OSError):
            print(f"Permission denied or error accessing directory: {directory}")


# Sentinel stored in debug_map when a keyword matched a tag but that tag is blacklisted.
# Distinct from None (unmatched) so callers can colour it differently.
_BLACKLISTED = "__blacklisted__"


class FileProcessor:
    def __init__(self, config, check_paused_or_stopped=None, callback=None, skip_paths=None):
        self.config = config
        self.llm_processor = LLMProcessor(config)
        
        if check_paused_or_stopped is None:
            self.check_paused_or_stopped = lambda: False
        else:
            self.check_paused_or_stopped = check_paused_or_stopped
            
        if callback is None:
            self.callback = print
        else:
            self.callback = callback
        self.failed_validations = []
        self.files_in_queue = 0
        self.total_processing_time = 0
        self.files_processed = 0
        self.files_completed = 0
        self._checkpoint_paths = set(skip_paths) if skip_paths else set()
        self._checkpoint_counter = 0

        # Zip extraction support
        # Maps temp_file_path → (composite_db_key, zip_source_name, zip_studio, [zip_performers])
        self._zip_file_map = {}
        _temp_folder = getattr(config, 'temp_folder', 'temp') or 'temp'
        self.temp_dir = Path(_temp_folder) if Path(_temp_folder).is_absolute() else Path(os.getcwd()) / _temp_folder

        # Progress state — updated throughout processing; emitted to the GUI via callback.
        self._progress = {
            'dirs_total':   0,   # total directories discovered by BackgroundIndexer
            'dirs_done':    0,   # directories fully processed so far
            'zips_total':   0,   # zip files in the current directory batch
            'zips_done':    0,   # zips completed in the current directory batch
            'images_total': 0,   # images in the current context (dir or zip)
            'images_done':  0,   # images processed in the current context
            'mode':         'dir',  # 'dir' → images bar shows directory images
                                    # 'zip' → images bar shows current zip images
        }

        # Database connection (populated below if output_mode includes 'db')
        self.db_conn   = None
        self.db_run_id = None
        # Set to True when a DB connection loss cannot be recovered; causes
        # check_pause_stop() to return True so the run stops cleanly.
        self._db_fatal = False

        if getattr(config, 'output_mode', 'json') in ('db', 'both'):
            try:
                self.db_conn = llmii_db.get_connection(
                    host     = getattr(config, 'db_host', 'localhost'),
                    port     = getattr(config, 'db_port', 5432),
                    user     = getattr(config, 'db_user', ''),
                    password = getattr(config, 'db_password', ''),
                    dbname   = getattr(config, 'db_name', ''),
                )
                self.db_run_id = llmii_db.create_tagger_run(
                    self.db_conn,
                    tagger_name='ImageIndexer',
                )
                print(f"Database connected. Tagger run id: {self.db_run_id}")
            except Exception as e:
                self.db_conn   = None
                self.db_run_id = None
                print(f"Database connection failed: {e}")
                print("Continuing without database output.")

        # If resuming and DB is available, load all previously-processed paths
        # so BackgroundIndexer can skip them entirely (no ExifTool reads needed).
        if getattr(config, 'resume_session', False) and self.db_conn:
            try:
                db_done = llmii_db.get_processed_paths(self.db_conn, config.directory)
                skip_paths = (skip_paths or set()) | db_done
                msg = f"Resume: {len(db_done):,} previously-processed files will be skipped."
                print(msg)
                if self.callback:
                    self.callback(msg)
            except Exception as e:
                print(f"Warning: resume DB query failed: {e}")

        self.image_processor = ImageProcessor(max_dimension=self.config.res_limit, patch_sizes=[14])

        print("Initializing ExifTool...")
        self.et = exiftool.ExifToolHelper(encoding='utf-8')
        print("ExifTool initialized successfully")

        # Load tag vocabulary from the configured file (project root)
        _root = Path(__file__).resolve().parent.parent
        _matched_log = _root / "matched_keywords.log"
        _unmatched_log = _root / "unmatched_keywords.log"
        _threshold = getattr(config, 'tag_fuzzy_threshold', TagMatcher.FUZZY_THRESHOLD)
        _tags_file = getattr(config, 'tags_file', 'mastertags.json')
        self.tag_matcher = TagMatcher(
            _root / _tags_file, name=_tags_file,
            matched_log_path=_matched_log, unmatched_log_path=_unmatched_log,
            fuzzy_threshold=_threshold,
        )
        # Fallback matcher disabled — single tags file in use
        self.tag_matcher_fallback = TagMatcher(
            _root / "__no_fallback__", name="fallback",
            matched_log_path=_matched_log, unmatched_log_path=_unmatched_log,
            fuzzy_threshold=_threshold,
        )

        # If a DB connection was established, replace the JSON-loaded tag
        # vocabulary with live data from the database and suppress duplicate
        # file-based logs (the DB captures raw/unmatched keywords instead).
        if self.db_conn:
            # Only the primary matcher is loaded from DB; the fallback
            # stays disabled (it would be identical data — no benefit).
            self.tag_matcher.load_from_db(self.db_conn)
            self.tag_matcher.suppress_file_logging = True
            self.tag_matcher_fallback.suppress_file_logging = True

        # Words in the prompt tend to get repeated back by certain models
        self.banned_words = ["no", "unspecified", "unknown", "unidentified", "identify", "topiary", "themes concepts", "items animals", "animals objects", "structures landmarks", "Foreground and background", "notable colors", "textures styles", "actions activities", "physical appearance", "Gender", "Age range", "visibly apparent", "apparent ancestry", "Occupation/role", "Relationships between individuals", "Emotions expressions", "body language"]
                
        self.keyword_fields = [
            "Keywords",
            "IPTC:Keywords",
            "Composite:keywords",
            "Subject",
            "DC:Subject",
            "XMP:Subject",
            "XMP-dc:Subject"
        ]
        self.caption_fields = [
            "Description",
            "XMP:Description",
            "ImageDescription",
            "DC:Description",
            "EXIF:ImageDescription",
            "Composite:Description",
            "Caption",
            "IPTC:Caption",
            "Composite:Caption"
            "IPTC:Caption-Abstract",
            "XMP-dc:Description",
            "PNG:Description"
        ]

        self.identifier_fields = [
            "Identifier",
            "XMP:Identifier",            
        ]
        self.status_fields = [
            "Status",
            "XMP:Status"
        ]
        self.filetype_fields = [
            "File:FileType",
            "File:FileTypeExtension"
        ]
        
        self.image_extensions = config.image_extensions
        self.metadata_queue = queue.Queue()

        chunk_size = getattr(config, 'chunk_size', 100)
        skip_folders = getattr(config, 'skip_folders', [])

        all_exts = [ext for exts in self.image_extensions.values() for ext in exts]
        ext_filter = getattr(config, 'image_extensions_filter', '')
        if ext_filter:
            allowed = {f".{e.strip().lower().lstrip('.')}" for e in ext_filter.split(',') if e.strip()}
            all_exts = [ext for ext in all_exts if ext.lower() in allowed]

        self.indexer = BackgroundIndexer(
            config.directory,
            self.metadata_queue,
            all_exts,
            config.no_crawl,
            chunk_size=chunk_size,
            skip_folders=skip_folders,
            skip_paths=skip_paths,
        )
        
        self.indexer.start()

    def rename_to_invalid(self, file_path):
        """ Rename a file to filename_ext.invalid
            Returns True if successful, False otherwise
        """
        try:
            # Clean up any exiftool temporary and backup files first
            dir_name = os.path.dirname(file_path)
            base_name = os.path.basename(file_path)

            # Look for various exiftool temporary file patterns
            # Standard pattern: filename_exiftool_tmp
            exiftool_tmp = file_path + "_exiftool_tmp"
            if os.path.exists(exiftool_tmp):
                try:
                    os.remove(exiftool_tmp)
                    self.callback(f"Cleaned up temporary file: {os.path.basename(exiftool_tmp)}")
                except Exception as e:
                    self.callback(f"Could not remove temp file {os.path.basename(exiftool_tmp)}: {str(e)}")

            # Check for backup files created by exiftool when -overwrite_original is not used
            # These have _original suffix
            backup_file = file_path + "_original"
            if os.path.exists(backup_file):
                # If the main file doesn't exist but the backup does, this IS the file to rename
                if not os.path.exists(file_path):
                    file_path = backup_file
                    base_name = os.path.basename(backup_file)
                else:
                    # Both exist - remove the backup
                    try:
                        os.remove(backup_file)
                        self.callback(f"Cleaned up backup file: {os.path.basename(backup_file)}")
                    except Exception as e:
                        self.callback(f"Could not remove backup file <{os.path.basename(backup_file)}>: {str(e)}")

            # Check if the file to rename exists
            if not os.path.exists(file_path):
                self.callback(f"File no longer exists, cannot rename: {base_name}")
                return False

            # Replace dots with underscores except the last one, then add .invalid
            # Format: filename_ext.invalid or filename_ext(N).invalid for duplicates
            name_parts = base_name.rsplit('.', 1)
            if len(name_parts) == 2:
                base_invalid_name = f"{name_parts[0]}_{name_parts[1]}"
            else:
                base_invalid_name = base_name

            new_path = os.path.join(dir_name, f"{base_invalid_name}.invalid")

            # If a file with this name already exists, add a counter in parentheses
            counter = 1
            original_new_path = new_path
            while os.path.exists(new_path):
                new_name_with_counter = f"{base_invalid_name}({counter}).invalid"
                new_path = os.path.join(dir_name, new_name_with_counter)
                counter += 1
                # Safety limit to avoid infinite loop
                if counter > 1000:
                    self.callback(f"Too many duplicate .invalid files, cannot rename: {base_name}")
                    return False

            # Rename the file
            os.rename(file_path, new_path)
            if new_path != original_new_path:
                self.callback(f"Renamed invalid file: {base_name} -> {os.path.basename(new_path)} (duplicate name)")
                print(f"Invalid or corrupt file <{base_name}> renamed to <{os.path.basename(new_path)}> (duplicate name)")
            else:
                self.callback(f"Renamed invalid file: {base_name} -> {os.path.basename(new_path)}")
                print(f"Invalid or corrupt file <{base_name}> renamed to <{os.path.basename(new_path)}>")
            return True
        except Exception as e:
            self.callback(f"Failed to rename invalid file <{file_path}>: {str(e)}")
            print(f"Failed: {str(e)}")
            return False

    def _get_sidecar_path(self, file_path):
        """Return the JSON sidecar path for the given image file.
        If config.sidecar_dir is set, embed the original full path in the filename so the
        source image can be located from the sidecar name alone.
        Example: C:\\temp\\sex\\dscn001.jpg -> <sidecar_dir>/c_temp_sex---dscn001.jpg.json
        """
        if self.config.sidecar_dir:
            abs_path = os.path.abspath(file_path).replace('\\', '/')
            dir_part = os.path.dirname(abs_path)
            base_name = os.path.basename(abs_path)
            # Remove colon (drive letter), replace slashes with underscores
            safe_dir = dir_part.replace(':', '').replace('/', '_').strip('_')
            sidecar_name = f"{safe_dir}---{base_name}.json"
            return os.path.join(self.config.sidecar_dir, sidecar_name)
        else:
            return os.path.splitext(file_path)[0] + ".json"

    def _read_json_sidecar(self, file_path):
        """Read a JSON sidecar file for the given image path.
        Returns a dict or None if the sidecar does not exist or cannot be read.
        """
        sidecar_path = self._get_sidecar_path(file_path)
        if not os.path.exists(sidecar_path):
            return None
        try:
            with open(sidecar_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading JSON sidecar for {os.path.basename(file_path)}: {e}")
            return None

    def _write_json_sidecar(self, file_path, metadata):
        """Write description, keywords, status and identifier to a JSON sidecar file.
        Returns True on success, False on failure.
        """
        sidecar_path = self._get_sidecar_path(file_path)
        if self.config.sidecar_dir:
            os.makedirs(self.config.sidecar_dir, exist_ok=True)
        try:
            data = {
                "Description": metadata.get("MWG:Description") or "",
                "Keywords": metadata.get("MWG:Keywords") or [],
                "Status": metadata.get("XMP:Status") or "",
                "Identifier": metadata.get("XMP:Identifier") or "",
            }
            with open(sidecar_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"JSON Sidecar Write Error: {os.path.basename(file_path)}")
            print(f"  Details: {str(e)}")
            self.callback(f"Error writing JSON sidecar: {str(e)}")
            return False

    def fix_file_extension(self, file_path, expected_ext):
        """ Fix file extension if it doesn't match the expected extension from metadata.
            Returns the new file path if renamed, or the original path if no change needed.
        """
        if not expected_ext:
            return file_path

        # Normalize the expected extension (lowercase, with leading dot)
        expected_ext = expected_ext.lower()
        if not expected_ext.startswith('.'):
            expected_ext = '.' + expected_ext

        # Get current extension
        current_ext = os.path.splitext(file_path)[1].lower()

        # If they match, no change needed
        if current_ext == expected_ext:
            return file_path

        try:
            # Build new path with correct extension
            base_path = os.path.splitext(file_path)[0]
            new_path = base_path + expected_ext

            # Check if target path already exists
            counter = 1
            original_new_path = new_path
            while os.path.exists(new_path):
                new_path = f"{base_path}({counter}){expected_ext}"
                counter += 1
                if counter > 1000:
                    self.callback(f"Too many files with same name, cannot rename: {os.path.basename(file_path)}")
                    return file_path

            # Rename the file
            os.rename(file_path, new_path)

            if new_path != original_new_path:
                print(f"Fixed extension: {os.path.basename(file_path)} -> {os.path.basename(new_path)} (duplicate name)")
                self.callback(f"Fixed extension: {os.path.basename(file_path)} -> {os.path.basename(new_path)}")
            else:
                print(f"Fixed extension: {os.path.basename(file_path)} -> {os.path.basename(new_path)}")
                self.callback(f"Fixed extension: {os.path.basename(file_path)} -> {os.path.basename(new_path)}")

            return new_path

        except Exception as e:
            self.callback(f"Failed to fix extension for {file_path}: {str(e)}")
            print(f"Extension fix failed: {str(e)}")
            return file_path

    def _write_checkpoint(self):
        """Persist processed paths to disk (non-DB mode only)."""
        try:
            data = {
                'directory': str(self.config.directory),
                'updated': datetime.now().isoformat(),
                'files_processed': self.files_processed,
                'processed_paths': list(self._checkpoint_paths),
            }
            _checkpoint_path().write_text(json.dumps(data), encoding='utf-8')
        except Exception as e:
            print(f"Warning: checkpoint write failed: {e}")

    def process_directory(self, directory):
        # Reload tag vocabulary from DB at the start of every run so that
        # aliases added via tag_review.py since the last run are picked up.
        if self.db_conn:
            try:
                self.tag_matcher.load_from_db(self.db_conn)
                print("Tag vocabulary reloaded from DB at run start.")
            except Exception as e:
                print(f"Tag reload warning (start of run): {e}")

        # Clear temp extraction folder at the start of every run to remove
        # stragglers from previous runs before any zip files are processed.
        self._clear_temp_dir()

        try:
            while not (self.indexer.indexing_complete and self.metadata_queue.empty()):
                if self.check_pause_stop():
                    return

                try:
                    directory, files = self.metadata_queue.get(timeout=1)
                    self.callback(f"Processing directory: {directory}")
                    self.callback(f"---")

                    # Separate zip files from regular images (sorted order already
                    # comes from BackgroundIndexer, but sort again to be safe).
                    image_files = sorted(f for f in files if not f.lower().endswith('.zip'))
                    zip_files   = sorted(f for f in files if f.lower().endswith('.zip'))

                    # Snapshot current directory count from indexer.
                    self._progress['dirs_total'] = max(
                        self._progress['dirs_total'],
                        self.indexer.total_directories,
                    )
                    self._progress['zips_total']   = len(zip_files)
                    self._progress['zips_done']    = 0
                    self._progress['mode']         = 'dir'
                    self._progress['images_total'] = len(image_files)
                    self._progress['images_done']  = 0
                    self._emit_progress()

                    def _on_file_done():
                        self._progress['images_done'] += 1
                        self._emit_progress()

                    # Process regular images first.
                    if self._process_file_list(image_files, on_file_done=_on_file_done):
                        return

                    # Process each zip sequentially: extract → process → cleanup.
                    for zip_path in zip_files:
                        extracted = self._extract_single_zip(zip_path)
                        if not extracted:
                            self._progress['zips_done'] += 1
                            self._emit_progress()
                            continue

                        self._progress['mode']         = 'zip'
                        self._progress['images_total'] = len(extracted)
                        self._progress['images_done']  = 0
                        self._emit_progress()

                        try:
                            if self._process_file_list(extracted, on_file_done=_on_file_done):
                                return
                        finally:
                            self._cleanup_zip_temp(zip_path)
                            self._progress['zips_done'] += 1
                            self._emit_progress()

                    self._progress['dirs_done'] += 1
                    self._emit_progress()
                    self.update_progress()

                except queue.Empty:
                    continue
        finally:
            try:
                self.et.terminate()
                self.callback("ExifTool process terminated cleanly")
            except Exception as e:
                self.callback(f"Warning: ExifTool termination error: {str(e)}")

            # Final checkpoint flush (non-DB mode)
            if getattr(self.config, 'output_mode', 'json') not in ('db', 'both') and self._checkpoint_paths:
                self._write_checkpoint()

            if self.db_conn and self.db_run_id:
                try:
                    llmii_db.finish_tagger_run(self.db_conn, self.db_run_id, status='success')
                    self.db_conn.close()
                    print("Database connection closed.")
                except Exception as e:
                    print(f"Warning: DB teardown error: {e}")

            
        
    def get_file_type(self, file_ext):
        """ If the filetype is supported, return the key
            so .nef would return RAW. Otherwise return
            None.
        """
        if not file_ext.startswith("."):
            file_ext = "." + file_ext
        
        file_ext = file_ext.lower()
        
        for file_type, extensions in self.image_extensions.items():
            if file_ext in [ext.lower() for ext in extensions]:
                
                return file_type
        
        return None

    # ------------------------------------------------------------------
    # Zip file helpers
    # ------------------------------------------------------------------

    def _emit_progress(self):
        """Send the current _progress snapshot to the GUI callback."""
        if self.callback:
            self.callback({'type': 'progress', **self._progress})

    def _clear_temp_dir(self):
        """Remove and recreate the temp extraction directory."""
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
            self.temp_dir.mkdir(parents=True, exist_ok=True)
            print(f"Temp directory cleared: {self.temp_dir}")
        except Exception as e:
            print(f"Warning: could not clear temp directory {self.temp_dir}: {e}")

    def _accepted_image_exts(self):
        """Return the set of image extensions (excl. .zip) that should be processed."""
        exts = {
            ext.lower()
            for cat, ext_list in self.image_extensions.items()
            if cat != 'ZIP'
            for ext in ext_list
        }
        ext_filter = getattr(self.config, 'image_extensions_filter', '')
        if ext_filter:
            allowed = {f".{e.strip().lower().lstrip('.')}" for e in ext_filter.split(',') if e.strip()}
            allowed.discard('.zip')
            if allowed:
                exts = exts & allowed
        return exts

    def _extract_single_zip(self, zip_path):
        """Extract one zip file to its own temp subdirectory.

        Checks whether each internal image has already been processed (DB mode)
        and only extracts images that still need work.  Registers each extracted
        file in ``self._zip_file_map``.

        Returns a list of extracted temp file paths (may be empty if every image
        is already done or the zip contains no matching images).
        """
        zip_abs = os.path.normpath(os.path.abspath(zip_path))
        zip_source_name = Path(zip_abs).name
        zip_stem = Path(zip_abs).stem
        zip_temp_dir = self.temp_dir / zip_stem
        accepted_img_exts = self._accepted_image_exts()
        extracted = []

        try:
            zip_studio, zip_performers = llmii_db.parse_zip_metadata(zip_abs)
        except Exception:
            zip_studio, zip_performers = None, []

        try:
            with zipfile.ZipFile(zip_abs, 'r') as zf:
                internal_images = [
                    info for info in zf.infolist()
                    if not info.is_dir()
                    and Path(info.filename).suffix.lower() in accepted_img_exts
                ]

                if not internal_images:
                    print(f"Zip contains no matching images (skipping): {zip_source_name}")
                    return []

                # Build composite keys for all internal images
                composites = {
                    info.filename: f"{zip_abs}::{info.filename}"
                    for info in internal_images
                }

                # In DB mode: batch-check which composites are already done
                already_done = set()
                if self.db_conn:
                    try:
                        db_status = llmii_db.get_image_status_batch(
                            self.db_conn, list(composites.values())
                        )
                        for composite_key, (db_id, db_st, db_kw_count) in db_status.items():
                            if db_st == 'success' and not self.config.reprocess_all:
                                # Sparse reprocess: skip only if keyword count meets threshold
                                if (self.config.reprocess_sparse
                                        and (db_kw_count or 0) < self.config.reprocess_sparse_min):
                                    pass  # fall through — don't add to already_done
                                else:
                                    already_done.add(composite_key)
                            elif db_st == 'failed' and not (
                                self.config.reprocess_failed or self.config.reprocess_all
                            ):
                                already_done.add(composite_key)
                    except Exception as e:
                        print(f"DB zip status check error for {zip_source_name}: {e}")

                to_extract = [i for i in internal_images if composites[i.filename] not in already_done]
                if not to_extract:
                    print(f"All images already processed in: {zip_source_name}")
                    return []

                print(f"Extracting {len(to_extract)}/{len(internal_images)} image(s) from: {zip_source_name}")
                zip_temp_dir.mkdir(parents=True, exist_ok=True)
                for info in to_extract:
                    composite_key = composites[info.filename]
                    try:
                        zf.extract(info, zip_temp_dir)
                    except Exception as e:
                        print(f"  Extract error ({info.filename} from {zip_source_name}): {e}")
                        continue

                    temp_file = str(zip_temp_dir / info.filename)
                    self._zip_file_map[os.path.normpath(temp_file)] = (
                        composite_key,
                        zip_source_name,
                        zip_studio,
                        zip_performers,
                    )
                    extracted.append(temp_file)

        except zipfile.BadZipFile:
            print(f"Warning: bad zip file (skipping): {zip_source_name}")
        except Exception as e:
            print(f"Warning: could not read zip {zip_source_name}: {e}")

        return extracted

    def _cleanup_zip_temp(self, zip_path):
        """Remove the temp extraction subdirectory for a single zip."""
        zip_stem = Path(zip_path).stem
        zip_temp_dir = self.temp_dir / zip_stem
        try:
            if zip_temp_dir.exists():
                shutil.rmtree(zip_temp_dir)
                print(f"Cleaned up temp dir: {zip_temp_dir}")
        except Exception as e:
            print(f"Warning: could not remove temp dir {zip_temp_dir}: {e}")

    def _process_file_list(self, files, on_file_done=None):
        """Process a list of files through ExifTool in batches of 50.

        on_file_done — optional zero-argument callable invoked after each file
                       finishes (used to update the progress counter).

        Returns True if processing was interrupted by a pause/stop request,
        False if all files completed normally.
        """
        if not files:
            return False

        batch_size = 50
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            print(f"Reading metadata for {len(batch)} file(s)...")
            metadata_list = self._get_metadata_batch(batch)

            for metadata in metadata_list:
                if metadata:
                    keywords = []
                    status = None
                    identifier = None
                    caption = None
                    validation_data = None
                    new_metadata = {}

                    # Check if we actually have a sidecar in the path
                    if self.config.use_sidecar and metadata["SourceFile"].lower().endswith(".xmp"):
                        metadata["SourceFile"] = os.path.splitext(metadata["SourceFile"])[0]

                    new_metadata["SourceFile"] = metadata.get("SourceFile")

                    # Inject zip-specific metadata if this file came from a zip.
                    source_norm = os.path.normpath(new_metadata["SourceFile"] or '')
                    if source_norm in self._zip_file_map:
                        _composite, _zip_src, _zip_studio, _zip_perfs = \
                            self._zip_file_map[source_norm]
                        new_metadata['_zip_db_key']     = _composite
                        new_metadata['_zip_source']     = _zip_src
                        new_metadata['_zip_studio']     = _zip_studio
                        new_metadata['_zip_performers'] = _zip_perfs

                    # Extract validation data if present
                    if not self.config.skip_verify and "ExifTool:Validate" in metadata:
                        validation_data = metadata.get("ExifTool:Validate")

                    filetype = None
                    filetype_ext = None
                    for key, value in metadata.items():
                        if key in self.keyword_fields:
                            keywords.extend(value)
                        if key in self.caption_fields:
                            caption = value
                        if key in self.identifier_fields:
                            identifier = value
                        if key in self.status_fields:
                            status = value
                        if key == "File:FileType":
                            filetype = value
                        if key == "File:FileTypeExtension":
                            filetype_ext = value

                    if keywords:
                        new_metadata["MWG:Keywords"] = keywords
                    if caption:
                        new_metadata["MWG:Description"] = caption
                    if status:
                        new_metadata["XMP:Status"] = status
                    if identifier:
                        new_metadata["XMP:Identifier"] = identifier
                    if validation_data:
                        new_metadata["ExifTool:Validate"] = validation_data
                    if filetype:
                        new_metadata["File:FileType"] = filetype
                    if filetype_ext:
                        new_metadata["File:FileTypeExtension"] = filetype_ext

                    self.files_processed += 1
                    self.process_file(new_metadata)
                    if on_file_done:
                        on_file_done()

                    # Checkpoint (non-DB mode): track processed path and flush periodically
                    if getattr(self.config, 'output_mode', 'json') not in ('db', 'both'):
                        src = new_metadata.get('SourceFile')
                        if src:
                            self._checkpoint_paths.add(os.path.normpath(src))
                            self._checkpoint_counter += 1
                            if self._checkpoint_counter >= 500:
                                self._write_checkpoint()
                                self._checkpoint_counter = 0

                if self.check_pause_stop():
                    return True

        return False

    def check_uuid(self, metadata, file_path):
        """ Very important or we end up processing 
            files more than once
        """ 
        try:
            status = metadata.get("XMP:Status")
            identifier = metadata.get("XMP:Identifier")
            keywords = metadata.get("MWG:Keywords")
            caption = metadata.get("MWG:Description")
            
            # Orphan check
            if identifier and self.config.reprocess_orphans and keywords and not status:
                    metadata["XMP:Status"] = "success"                    
                    status = "success"
                    try:
                        written = self.write_metadata(file_path, metadata)
                        
                        if written and not self.config.reprocess_all:
                            
                            print(f"Status added for orphan: {file_path}")  
                            self.callback(f"Status added for orphan: {file_path}")
                            
                        else:
                            print(f"Metadata write error for orphan: {file_path}")
                            self.callback(f"Metadata write error for orphan: {file_path}")
                            return None
                    except Exception as e:
                        print(f"Error writing orphan status: {e}")
                        return None
        
            # Does file have a UUID in metadata
            if identifier:
                if not self.config.reprocess_all and status == "success":
                    # Sparse reprocess: flag images with fewer than N matched keywords
                    if (self.config.reprocess_sparse
                            and isinstance(keywords, list)
                            and len(keywords) < self.config.reprocess_sparse_min):
                        metadata["XMP:Status"] = None
                        return metadata
                    return None
                    
                # If it is retry, do it again
                if self.config.reprocess_all or status == "retry":
                    metadata["XMP:Status"] = None
                    
                    return metadata
                
                # If it is fail, don't do it unless we specifically want to
                if status == "failed":
                    if self.config.reprocess_failed or self.config.reprocess_all:
                        metadata["XMP:Status"] = None
                        
                        return metadata                    
                    
                    else:
                        return None
                
                # If there are no keywords, processs it                
                if not keywords:
                    metadata["XMP:Status"] = None
                    
                    return metadata
                
                else:
                    return None
                
            # No UUID, treat as new file
            else:
                metadata["XMP:Identifier"] = str(uuid.uuid4())
                
                return metadata  # New file

        except Exception as e:
            print(f"Error checking UUID: {str(e)}")
            
            return None
                        
    def check_pause_stop(self):
        if self._db_fatal:
            return True
        if self.check_paused_or_stopped():

            while self.check_paused_or_stopped():
                time.sleep(0.1)

            if self.check_paused_or_stopped():
                return True

        return False

    def _try_reconnect_db(self, max_attempts=3):
        """Try to re-establish the DB connection after a failure.

        Closes any stale connection, opens a fresh one (without re-running
        migrations), and creates a new tagger_run row.  Returns True on
        success, False if all attempts fail.
        """
        if self.db_conn is not None and not self.db_conn.closed:
            try:
                self.db_conn.close()
            except Exception:
                pass

        for attempt in range(1, max_attempts + 1):
            try:
                print(f"DB reconnect attempt {attempt}/{max_attempts}...")
                self.db_conn = llmii_db.get_connection(
                    host=getattr(self.config, 'db_host', 'localhost'),
                    port=getattr(self.config, 'db_port', 5432),
                    user=getattr(self.config, 'db_user', ''),
                    password=getattr(self.config, 'db_password', ''),
                    dbname=getattr(self.config, 'db_name', ''),
                    apply_schema_migrations=False,
                )
                self.db_run_id = llmii_db.create_tagger_run(
                    self.db_conn, tagger_name='ImageIndexer',
                )
                print(f"DB reconnected. New run id: {self.db_run_id}")
                return True
            except Exception as e:
                print(f"  Reconnect attempt {attempt} failed: {e}")
                self.db_conn = None
                self.db_run_id = None

        return False

    def _get_metadata_batch(self, files):
        """ Get metadata for a batch of files
            using persistent ExifTool instance.
        """
        exiftool_fields = self.keyword_fields + self.caption_fields + self.identifier_fields + self.status_fields + self.filetype_fields
        
        try:
            if self.config.skip_verify:
                params = []
            else:
                params = ["-validate"]   
            
            # Use sidecars if they exist for metadata instead of images because
            # that is where we will have put the UUID and Status info
            if self.config.use_sidecar:
                xmp_files = []
                for file in files:
                    
                    # Check for files named file.ext.xmp for sidecar
                    if os.path.exists(file + ".xmp"):
                       xmp_files.append(file  + ".xmp")

                    else:
                        xmp_files.append(file)
                files = xmp_files
            results = self.et.get_tags(files, tags=exiftool_fields, params=params)

            # Always source content/tracking fields from JSON sidecar instead of EXIF
            fields_to_clear = set(self.keyword_fields + self.caption_fields +
                                  self.identifier_fields + self.status_fields)
            for meta in results:
                source_file = meta.get("SourceFile")
                if not source_file:
                    continue
                for key in list(meta.keys()):
                    if key in fields_to_clear:
                        del meta[key]
                sidecar_data = self._read_json_sidecar(source_file)
                if sidecar_data:
                    if sidecar_data.get("Description"):
                        meta["Description"] = sidecar_data["Description"]
                    if sidecar_data.get("Keywords"):
                        meta["Keywords"] = sidecar_data["Keywords"]
                    if sidecar_data.get("Status"):
                        meta["XMP:Status"] = sidecar_data["Status"]
                    if sidecar_data.get("Identifier"):
                        meta["XMP:Identifier"] = sidecar_data["Identifier"]

            # In DB mode, for any file that still has no identifier after the
            # sidecar check, look it up in the database.  A single batch query
            # covers all files in this chunk so there is no per-file round-trip.
            if self.db_conn:
                source_files = [
                    m.get("SourceFile") for m in results
                    if m.get("SourceFile") and not m.get("XMP:Identifier")
                ]
                if source_files:
                    try:
                        db_status = llmii_db.get_image_status_batch(self.db_conn, source_files)
                        for meta in results:
                            sf = meta.get("SourceFile")
                            if sf and sf in db_status and not meta.get("XMP:Identifier"):
                                identifier, status, kw_count = db_status[sf]
                                if identifier:
                                    meta["XMP:Identifier"] = identifier
                                if status:
                                    # Sparse reprocess: clear status so _check_uuid_status
                                    # re-processes images with too few matched keywords.
                                    if (status == 'success'
                                            and self.config.reprocess_sparse
                                            and (kw_count or 0) < self.config.reprocess_sparse_min):
                                        meta["XMP:Status"] = None
                                    else:
                                        meta["XMP:Status"] = status
                    except Exception as e:
                        print(f"DB status lookup error: {e}")

            return results

        except exiftool.exceptions.ExifToolExecuteError as e:
            print(f"ExifTool Execute Error: {str(e)}")
            self.callback(f"ExifTool execute error - check if files are accessible")
            return []
        except exiftool.exceptions.ExifToolVersionError as e:
            print(f"ExifTool Version Error: {str(e)}")
            print("  Please update ExifTool to a compatible version")
            return []
        except Exception as e:
            print(f"ExifTool Error: {type(e).__name__} - {str(e)}")
            return []

    def update_progress(self):
        files_processed = self.files_processed
        files_remaining = self.indexer.total_files_found - files_processed
        
        if files_remaining < 0:
            files_remaining = 0
        
        self.callback(f"Directory processed. Files remaining in queue: {files_remaining}")
        self.callback(f"---")
        
    
    def process_file(self, metadata):
        """ Process a file and update its metadata in one operation.
            This minimizes the number of writes to the file.
        """
        try:

            success = True
            file_path = metadata["SourceFile"]

            # If the file doesn't exist anymore, skip it
            if not os.path.exists(file_path):
                self.callback(f"File no longer exists: {file_path}")
                self.callback(f"---")
                return

            # Check if file is already marked as invalid - skip it entirely
            # Unless reprocess_all is enabled
            current_status = metadata.get("XMP:Status")
            if current_status == "invalid" and not self.config.reprocess_all:
                self.callback(f"Skipping file marked as invalid: {file_path}")
                self.callback(f"---")
                return

            # Only run validation check for files without a status or if reprocess_all
            # This prevents re-validating files that are already success/failed/retry/valid
            should_validate = (not current_status or self.config.reprocess_all) and not self.config.skip_verify

            if should_validate:
                validation_parts = metadata.get("ExifTool:Validate", "0 0 0").split()
                if len(validation_parts) >= 3:
                    errors, warnings, minor = map(int, validation_parts[:3])
                else:
                    errors, warnings, minor = 0, 0, 0

                # If there are validation errors, mark as invalid and skip
                if errors > 0:
                    print(f"Validation Failed: {os.path.basename(file_path)}")
                    print(f"  Errors: {errors}, Warnings: {warnings}, Minor: {minor}")
                    self.callback(f"\nValidation failed: {file_path}")
                    self.callback(f"  Errors: {errors}, Warnings: {warnings}, Minor: {minor}")
                    self.failed_validations.append(file_path)
                    if self.config.rename_invalid:
                        self.rename_to_invalid(file_path)
                    self.callback(f"---")
                    return

                # If there are warnings, test if we can write to the file
                # This prevents wasting LLM processing on unwritable files
                if (warnings > 0) and (minor >= warnings):
                    print(f"File has validation warnings: {os.path.basename(file_path)}")
                    print(f"  Warnings: {warnings}, Minor: {minor} - Testing writeability...")
                    test_metadata = {"SourceFile": file_path, "XMP:Status": "valid"}
                    if not self.write_metadata(file_path, test_metadata):
                        print(f"  Metadata cannot be written to file")
                        self.callback(f"\nMetadata is not writable: {file_path}")
                        self.failed_validations.append(file_path)
                        self.callback(f"---")
                        return
                    print(f"  File is writable - proceeding")
                    # File is writable, update metadata to reflect valid status
                    metadata["XMP:Status"] = "valid"

            metadata = self.check_uuid(metadata, file_path)
            if not metadata:
                return
                
            image_type = self.get_file_type(os.path.splitext(file_path)[1].lower())
            if image_type is None:
                self.callback(f"Not a supported image type: {file_path}")
                self.callback(f"---")
                return

            filetype = metadata.get("File:FileType", image_type)
            print(f"Processing: {os.path.basename(file_path)} [{filetype}]")

            start_time = time.time()

            try:
                processed_image, image_path = self.image_processor.process_image(file_path)
            except Exception as e:
                print(f"Image Processing Error: {os.path.basename(file_path)}")
                print(f"  Error type: {type(e).__name__}")
                print(f"  Details: {str(e)}")
                self.callback(f"Image processing error for {file_path}: {str(e)}")
                if self.config.rename_invalid:
                    self.rename_to_invalid(file_path)
                self.callback(f"---")
                return

            if not processed_image:
                print(f"Image Processing Failed: {os.path.basename(file_path)}")
                print(f"  Could not generate base64 image data")
                self.callback(f"Failed to process image: {file_path}")
                if self.config.rename_invalid:
                    self.rename_to_invalid(file_path)
                self.callback(f"---")
                return

            updated_metadata = self.generate_metadata(metadata, processed_image)
           
            status = updated_metadata.get("XMP:Status")
            
            # Retry one time if failed
            if not self.config.quick_fail and status == "retry":
                print(f"AI Generation Issue - Retrying: {os.path.basename(file_path)}")
                print(f"  Reason: No valid keywords generated on first attempt")
                self.callback(f"Asking AI to try again for {file_path}...")
                self.callback(f"---")
                updated_metadata = self.generate_metadata(metadata, processed_image)
                status = updated_metadata.get("XMP:Status")

            # If retry didn't work, mark failed
            if not status == "success":
                print(f"AI Generation Failed: {os.path.basename(file_path)}")
                print(f"  The AI could not generate valid keywords after retry")
                self.callback(f"Retry failed due to AI for {file_path}")
                self.callback(f"---")
                metadata["XMP:Status"] = "failed"
                
                if not self.config.dry_run:
                    success = False
                    self.write_metadata(file_path, metadata)
                
                
            # Fix file extension if enabled (before writing metadata)
            if self.config.fix_extension and success:
                expected_ext = metadata.get("File:FileTypeExtension")
                if expected_ext:
                    new_file_path = self.fix_file_extension(file_path, expected_ext)
                    if new_file_path != file_path:
                        file_path = new_file_path
                        updated_metadata["SourceFile"] = file_path

            # Send image data to callback for GUI display
            if self.callback and hasattr(self.callback, '__call__') and success:

                # Derive studio / performers for display.
                # Zip images carry these in the original metadata dict.
                # Regular images fall back to path-based parsing.
                _studio     = metadata.get('_zip_studio') or ''
                _performers = list(metadata.get('_zip_performers') or [])
                if not _performers:
                    try:
                        _, _performers = llmii_db.parse_gallery_and_performers(file_path)
                    except Exception:
                        _performers = []
                if not _studio:
                    _path_parts = Path(file_path).parts
                    if any(p.lower().replace(' ', '') == 'suicidegirls' for p in _path_parts):
                        _studio = 'Suicide Girls'

                # Create a dictionary with image data for GUI
                image_data = {
                    'type': 'image_data',
                    'base64_image': processed_image,
                    'caption': updated_metadata.get('MWG:Description', ''),
                    'keywords': updated_metadata.get('MWG:Keywords', []),
                    'raw_keywords': updated_metadata.get('_raw_keywords', []),
                    'debug_map': updated_metadata.get('_debug_map', {}),
                    'file_path': file_path,
                    'studio': _studio,
                    'performers': _performers,
                }

                self.callback(image_data)

            if not self.config.dry_run and success:
                # Carry forward zip routing fields that generate_metadata doesn't know about.
                for _k in ('_zip_db_key', '_zip_source', '_zip_studio', '_zip_performers'):
                    if _k in metadata:
                        updated_metadata[_k] = metadata[_k]
                write_success = self.write_metadata(file_path, updated_metadata)
                if write_success:
                    print(f"  Metadata written successfully")
                    success = True
                else:
                    success = False
                    #print(f"Could not write new metadata to file: {file_path}") 
                    #self.callback(f"Failed writing metadata for {file_path}")
                    #self.callback(f"---")
                    
            end_time = time.time()
            processing_time = end_time - start_time
            self.total_processing_time += processing_time
            self.files_completed += 1

            # Reload tag vocabulary from DB every 15 completions so that
            # aliases added via tag_review.py mid-run are picked up promptly.
            if self.db_conn and not self.db_conn.closed and self.files_completed % 15 == 0:
                try:
                    self.tag_matcher.load_from_db(self.db_conn)
                    print(f"Tag vocabulary reloaded from DB ({self.files_completed} files complete).")
                except Exception as e:
                    print(f"Tag reload warning (mid-run): {e}")

            # Calculate and display progress info
            in_queue = self.indexer.total_files_found - self.files_processed
            average_time = self.total_processing_time / self.files_completed
            time_left = average_time * in_queue
            time_left_unit = "s"
            
            if time_left > 180:
                time_left = time_left / 60
                time_left_unit = "mins"
            
            if time_left < 0:
                time_left = 0
            
            if in_queue < 0:
                in_queue = 0
            if success:
                 
                self.callback(f"<b>Image:</b> {os.path.basename(file_path)}")
                self.callback(f"<b>Status:</b> {status}")

                self.callback(
                    f"<b>Processing time:</b> {processing_time:.2f}s, <b>Average processing time:</b> {average_time:.2f}s"
                )
                self.callback(
                    f"<b>Processed:</b> {self.files_processed}, <b>In queue:</b> {in_queue}, <b>Time remaining (est):</b> {time_left:.2f}{time_left_unit}"
                )
                self.callback("---")   
                
            if self.check_pause_stop():
                return
            
        except Exception as e:
            print(f"Processing Error: {os.path.basename(file_path)}")
            print(f"  Error type: {type(e).__name__}")
            print(f"  Details: {str(e)}")
            self.callback(f"<b>Error processing:</b> {file_path}: {str(e)}")
            self.callback(f"---")
            return
    
    def generate_metadata(self, metadata, processed_image):
        """ Generate metadata without writing to file.
            Returns (metadata_dict)
            
            short_caption will get a short caption in a single generation
            
            detailed_caption will get get a detailed caption using two
            generations
            
            update_caption appends new caption to existing caption to the existing description.
            
        """
        new_metadata = {}
        existing_caption = metadata.get("MWG:Description")
        caption = None
        keywords = None
        detailed_caption = ""
        old_keywords = metadata.get("MWG:Keywords", [])
        file_path = metadata["SourceFile"]
        
        try:

            # Determine whether to generate caption, keywords, or both
            if not self.config.no_caption and self.config.detailed_caption:
                # Stage 1: image → verbose description
                detailed_caption = clean_string(self.llm_processor.describe_content(task="caption", processed_image=processed_image))

                if existing_caption and self.config.update_caption:
                    caption = existing_caption + "<generated>" + detailed_caption + "</generated>"
                else:
                    caption = detailed_caption

                # Stage 2: description text → keywords (no image — grounded in description)
                if detailed_caption:
                    data = clean_tags(self.llm_processor.describe_content(task="keywords_from_text", description=detailed_caption))
                else:
                    # Caption failed — fall back to direct image → keywords
                    print(f"  Caption generation failed, falling back to direct keyword extraction")
                    data = clean_tags(self.llm_processor.describe_content(task="keywords", processed_image=processed_image))

                if isinstance(data, dict):
                    keywords = data.get("Keywords")
                elif isinstance(data, list) and data and all(isinstance(item, str) for item in data):
                    seen = set()
                    keywords = [k for k in data if not (k.lower() in seen or seen.add(k.lower()))][:30]

            else:
                if self.config.no_caption:
                    data = clean_tags(self.llm_processor.describe_content(task="keywords", processed_image=processed_image))
                else:
                    data = clean_json(self.llm_processor.describe_content(task="caption_and_keywords", processed_image=processed_image))
                         
                if isinstance(data, dict):
                    keywords = data.get("Keywords")

                    if not existing_caption and not self.config.no_caption:
                        caption = data.get("Description")

                    elif existing_caption and self.config.update_caption:
                        caption = existing_caption + "<generated>" + data.get("Description") + "</generated>"

                    elif data.get("Description") and not self.config.no_caption:
                        caption = data.get("Description")

                    else:
                        if existing_caption:
                            caption = existing_caption
                        else:
                            caption = ""

                elif isinstance(data, list) and data and all(isinstance(item, str) for item in data):
                    # Model returned a raw keyword array instead of {"Description": ..., "Keywords": [...]}
                    seen = set()
                    keywords = [k for k in data if not (k.lower() in seen or seen.add(k.lower()))][:30]
                    caption = existing_caption or ""
                        
            if not keywords:
                print(f"No Keywords Generated: {os.path.basename(file_path)}")
                print(f"  AI response did not contain valid keywords")
                status = "retry"

            else:
                status = "success"
                raw_keywords = list(keywords)
                keywords, debug_map = self.process_keywords(metadata, keywords, return_debug=True)
                _G = '\033[32m'   # green       = matched
                _Y = '\033[33m'   # dark yellow = blacklisted
                _R = '\033[31m'   # red         = unmatched
                _Z = '\033[0m'    # reset
                print(f"--- {os.path.basename(file_path)} ---")
                if caption:
                    print(f"  Description: {caption}")
                def _kw_color(kw):
                    v = debug_map.get(kw)
                    if v is _BLACKLISTED:
                        return _Y
                    return _G if v else _R
                raw_parts = [
                    f"{_kw_color(kw)}{kw}{_Z}"
                    for kw in raw_keywords
                ]
                print(f"  Raw ({len(raw_keywords)}): {', '.join(raw_parts)}")
                if keywords:
                    print(f"  Matched ({len(keywords)}): {', '.join(keywords)}")

            new_metadata["MWG:Description"] = caption
            new_metadata["MWG:Keywords"] = keywords
            new_metadata["_raw_keywords"] = raw_keywords if status == "success" else []
            new_metadata["_debug_map"] = debug_map if status == "success" else {}
            new_metadata["XMP:Status"] = status
            new_metadata["XMP:Identifier"] = metadata.get("XMP:Identifier", str(uuid.uuid4()))
            new_metadata["SourceFile"] = file_path
            
            return new_metadata
            
        except Exception as e:
            print(f"Metadata Generation Error: {os.path.basename(file_path)}")
            print(f"  Error type: {type(e).__name__}")
            print(f"  Details: {str(e)}")
            self.callback(f"Parse error for {file_path}: {str(e)}")
            self.callback(f"---")
            metadata["XMP:Status"] = "retry"

            return metadata
            
    def write_metadata(self, file_path, metadata):
        """Write metadata to JSON sidecar and/or PostgreSQL depending on output_mode.

        For images extracted from zip archives, the metadata dict contains
        ``_zip_db_key`` (the composite DB path) and ``_zip_source`` (zip filename).
        The JSON sidecar is written to the temp file location; the DB row uses
        the composite key so subsequent runs can identify already-processed images.
        """
        if self.config.dry_run:
            print("Dry run. Not writing.")
            return True

        output_mode = getattr(self.config, 'output_mode', 'json')
        success = True

        # Determine the DB path: composite key for zip images, real path otherwise.
        db_path   = metadata.get('_zip_db_key') or file_path
        zip_source = metadata.get('_zip_source') or None

        # JSON sidecar write (uses the actual temp file path, not the composite key)
        if output_mode in ('json', 'both'):
            try:
                if not self._write_json_sidecar(file_path, metadata):
                    success = False
            except Exception as e:
                print(f"Metadata Write Error: {os.path.basename(file_path)}")
                print(f"  Error type: {type(e).__name__}")
                print(f"  Details: {str(e)}")
                self.callback(f"\nError writing metadata: {str(e)}")
                if self.config.rename_invalid:
                    self.rename_to_invalid(file_path)
                success = False

        # Database write (uses composite key for zip images)
        if output_mode in ('db', 'both') and self.db_conn and self.db_run_id:
            try:
                llmii_db.write_image_to_db(
                    self.db_conn, db_path, metadata, self.db_run_id,
                    zip_source=zip_source,
                )
            except Exception as e:
                err_name = type(e).__name__
                print(f"DB Write Error: {os.path.basename(str(db_path))}")
                print(f"  Error type: {err_name}")
                print(f"  Details: {str(e)}")

                # psycopg2 connection-level errors: the socket was closed by
                # the server (idle timeout, firewall, etc.).  Attempt reconnect.
                # Other errors are transaction-level failures — rollback and continue.
                _CONN_ERRORS = ('InterfaceError', 'OperationalError')
                if err_name not in _CONN_ERRORS:
                    # Transaction is in an aborted state — rollback so the
                    # connection is usable again for the next file.
                    try:
                        self.db_conn.rollback()
                    except Exception:
                        pass
                    self.callback(f"\nDB write error: {str(e)}")
                    success = False
                else:
                    self.callback(f"\nDB connection lost ({err_name}). Attempting reconnect...")
                    if self._try_reconnect_db():
                        # Retry the write on the fresh connection.
                        try:
                            llmii_db.write_image_to_db(
                                self.db_conn, db_path, metadata, self.db_run_id,
                                zip_source=zip_source,
                            )
                            self.callback(" Reconnected — write succeeded.")
                        except Exception as retry_e:
                            print(f"  Write failed after reconnect: {retry_e}")
                            self.callback(f"\nDB write failed after reconnect: {retry_e}")
                            success = False
                    else:
                        if output_mode == 'db':
                            msg = (
                                "\nDB connection could not be restored after 3 attempts. "
                                "Processing stopped (DB-only mode). "
                                "Switch output mode to 'both' or 'json' to continue without the database."
                            )
                            self._db_fatal = True
                        else:
                            msg = (
                                "\nDB connection could not be restored after 3 attempts. "
                                "Falling back to JSON-only output for the remainder of this run."
                            )
                        self.callback(msg)
                        print(msg)

        return success
    
    def process_keywords(self, metadata, new_keywords, return_debug=False):
        """ Normalize extracted keywords and deduplicate them.
            If update is configured, combine the old and new keywords.
            When a TagMatcher is active, each keyword is matched against the tag
            vocabulary: matched keywords are replaced with the canonical tag;
            unmatched keywords are discarded and logged.

            When return_debug=True, returns (keywords, debug_map) where
            debug_map maps each raw new_keyword to its resolved tag or None.
        """
        all_keywords = set()
        blacklist = [w.strip().lower() for w in self.config.tag_blacklist if w.strip()]

        # Hard cap: no alias in either tag file is longer than 5 words.
        # Keywords exceeding this are combinatorial garbage from the LLM and
        # would never match anything — skip immediately and log them.
        _MAX_MATCH_WORDS = 5

        # Prefixes that indicate the model is describing an absent attribute.
        _NEGATIVE_RE = re.compile(r'^(no|not|without)\s', re.IGNORECASE)

        # Prefixes that indicate the model is hedging / uncertain.
        # "possibly nude", "appears to be kneeling", "may have tattoos" etc.
        # are low-confidence tags that are more often wrong than right — discard them.
        _UNCERTAIN_RE = re.compile(
            r'^(possibly|perhaps|probably|appears?\s+to\s+(be|have)|'
            r'seems?\s+to\s+(be|have)|may\s+(be|have)|might\s+(be|have)|'
            r'could\s+(be|have)|likely\s|seemingly\s|'
            r'looks?\s+(like|as\s+if))\b',
            re.IGNORECASE,
        )

        # Content-level classification tags emitted by the tag instruction.
        # These are self-contained values that have no alias in the tag vocabulary
        # — pass them through directly without requiring a DB match.
        _CONTENT_LEVEL_TAGS = frozenset({'sfw', 'nudity', 'explicit'})

        # Multi-color hair normalizer — canonicalises any "X and Y hair" /
        # "X-Y hair" / "Y X hair" variant to alphabetically sorted
        # "X and Y hair" so all colour combos resolve to one form.
        _HAIR_COLOR_WORDS = frozenset({
            'black', 'white', 'red', 'blue', 'green', 'yellow', 'pink', 'purple',
            'orange', 'brown', 'grey', 'gray', 'silver', 'gold', 'golden', 'teal',
            'turquoise', 'lavender', 'violet', 'magenta', 'cyan', 'coral', 'amber',
            'blonde', 'brunette', 'auburn', 'platinum', 'strawberry', 'ash',
            'copper', 'chestnut', 'caramel', 'honey', 'champagne', 'ombre',
            # NOTE: shade modifiers like 'dark', 'light', 'bright', 'neon', 'pastel'
            # are intentionally excluded.  "dark red hair" is a single compound colour,
            # not two separate colours "dark" + "red".  Including modifiers here caused
            # e.g. "dark red hair" to be rewritten to "dark and red hair", which then
            # failed to match the alias "dark red hair" in the database.
        })
        # Regex to detect a hair colour keyword (ends in "hair", contains a colour)
        _MULTI_HAIR_RE = re.compile(r'\bhair\b', re.I)

        # Colours that only occur in dyed/unnatural hair.
        # Natural colours (black, brown, blonde, auburn, red, gray, etc.) are
        # intentionally absent so they continue to resolve via the normal alias lookup.
        _UNNATURAL_HAIR_COLORS = frozenset({
            'blue', 'green', 'purple', 'violet', 'pink', 'teal', 'turquoise',
            'lavender', 'magenta', 'cyan', 'coral', 'orange', 'yellow',
            'silver', 'gold', 'golden',
        })

        def _normalize_hair(kw: str):
            """Normalise multi-colour hair keywords to sorted 'A and B hair' form.
            Only acts when the keyword ends in 'hair' and contains 2+ colour words
            (joined by 'and', '-', or bare space).  Single-colour hair like
            'red hair' is left unchanged — colour stripping is not applied to hair
            because colour IS the tag there."""
            k = kw.lower().strip()
            if not k.endswith('hair'):
                return None
            # Extract everything before the trailing " hair"
            prefix = re.sub(r'\s*hair\s*$', '', k).strip()
            if not prefix:
                return None
            # Split on ' and ', '-', or plain space to get candidate tokens
            tokens = re.split(r'\s+and\s+|\s*-\s*|\s+', prefix)
            # Filter to only recognised colour words (drops 'streaked', 'tipped' etc.)
            colors = [t for t in tokens if t in _HAIR_COLOR_WORDS]
            if len(colors) < 2:
                return None  # single colour or unrecognised — leave as-is
            # Sort alphabetically for a stable canonical form
            canonical = ' and '.join(sorted(set(colors))) + ' hair'
            return canonical if canonical != k else None

        def _resolve_colored_hair(kw: str):
            """Map unnatural-colored hair keywords to canonical tags.

            Returns 'Colored Hair' for a single unnatural colour (e.g. 'blue hair',
            'light blue hair', 'neon pink hair') and 'Multicolored Hair' for two or
            more unnatural colours or any mix that includes at least one unnatural
            colour alongside another colour (e.g. 'blue and green hair',
            'blue-purple hair', 'red and blue hair').

            Returns None for natural hair colours (black, brown, blonde, red, etc.)
            so those continue through the normal alias-lookup path.

            Call this AFTER _normalize_hair() so that multi-colour forms such as
            'blue-green hair' have already been rewritten to 'blue and green hair'.
            """
            k = kw.lower().strip()
            if not k.endswith('hair'):
                return None
            prefix = re.sub(r'\s*hair\s*$', '', k).strip()
            if not prefix:
                return None
            # Split on ' and ', '-', or plain spaces to recover individual tokens
            tokens = re.split(r'\s+and\s+|\s*-\s*|\s+', prefix)
            unnatural = [t for t in tokens if t in _UNNATURAL_HAIR_COLORS]
            if not unnatural:
                return None  # all-natural colouring — leave for normal alias lookup
            # Two or more recognized colour words, or explicit 'and' between colours
            all_colors = [t for t in tokens if t in _HAIR_COLOR_WORDS]
            if len(all_colors) >= 2 or ' and ' in prefix:
                return 'Multicolored Hair'
            return 'Colored Hair'

        # Piercing location normalizer — mirrors the tattoo normalizer.
        # Converts "nipple ring", "pierced nipples", "nipple piercing" etc. to
        # canonical "Piercing - Location" form.  Ordered specific → general.
        _PIERCING_LOCATIONS = [
            (re.compile(r'\bnipple', re.I),                  'Nipple'),
            (re.compile(r'\bbelly\s+button|navel', re.I),    'Navel'),
            (re.compile(r'\bseptum', re.I),                  'Septum'),
            (re.compile(r'\bnostril|nose\b', re.I),          'Nose'),
            (re.compile(r'\btongue', re.I),                  'Tongue'),
            (re.compile(r'\beyebrow', re.I),                 'Eyebrow'),
            (re.compile(r'\bbridge', re.I),                  'Bridge'),
            (re.compile(r'\blip\b|labret|monroe', re.I),     'Lip'),
            (re.compile(r'\bclit|clitoris|genital', re.I),   'Genital'),
            (re.compile(r'\bindustrial', re.I),              'Industrial'),
            (re.compile(r'\bhelix|tragus|daith|rook|conch', re.I), 'Ear'),
            (re.compile(r'\bear\b|earring', re.I),           'Ear'),
            (re.compile(r'\bnavel|belly', re.I),             'Navel'),
            (re.compile(r'\bcollarbone|clavicle', re.I),     'Collarbone'),
            (re.compile(r'\bneck', re.I),                    'Neck'),
            (re.compile(r'\bbrow|forehead', re.I),           'Eyebrow'),
            (re.compile(r'\bfinger', re.I),                  'Finger'),
            (re.compile(r'\bnavel', re.I),                   'Navel'),
        ]

        def _normalize_piercing(kw: str):
            """If kw describes a piercing/ring at a body location, return
            'Piercing - Location'.  Returns None for non-piercing keywords."""
            k = kw.lower()
            has_piercing = ('piercing' in k or 'pierced' in k
                            or 'ring' in k or 'earring' in k or 'stud' in k)
            if not has_piercing:
                return None
            # Avoid false-positive on "ring" buried inside unrelated words,
            # but only when earring isn't the actual trigger.
            if 'ring' in k and 'earring' not in k and not re.search(r'\b(ring|rings)\b', k):
                return None
            for pattern, location in _PIERCING_LOCATIONS:
                if pattern.search(k):
                    return f'Piercing - {location}'
            return None  # piercing mentioned but location unknown — pass through

        # Tattoo location normalizer — converts any tattoo keyword that references
        # a body location into canonical "Tattoo - Location" form regardless of
        # how the model phrased it ("arm tattoo", "tattoo on left arm", "tattooed
        # arms", etc.).  More-specific locations are listed first so "forearm"
        # wins over "arm" and "shoulder blade" wins over "shoulder".
        _TATTOO_LOCATIONS = [
            # multi-word / specific
            (re.compile(r'\bshoulder\s+blade', re.I), 'Shoulder Blade'),
            (re.compile(r'\bforearm', re.I),           'Forearm'),
            (re.compile(r'\bupper\s+arm', re.I),       'Upper Arm'),
            (re.compile(r'\bbicep', re.I),             'Bicep'),
            (re.compile(r'\blower\s+back', re.I),      'Lower Back'),
            (re.compile(r'\bupper\s+back', re.I),      'Upper Back'),
            (re.compile(r'\blower\s+abdomen', re.I),   'Abdomen'),
            (re.compile(r'\bupper\s+chest', re.I),     'Chest'),
            (re.compile(r'\blower\s+chest', re.I),     'Chest'),
            (re.compile(r'\binner\s+thigh', re.I),     'Thigh'),
            (re.compile(r'\bupper\s+thigh', re.I),     'Thigh'),
            (re.compile(r'\blower\s+leg', re.I),       'Leg'),
            (re.compile(r'\brib\s*cage', re.I),        'Ribcage'),
            # single-word
            (re.compile(r'\barm\b', re.I),             'Arm'),
            (re.compile(r'\barms\b', re.I),            'Arm'),
            (re.compile(r'\bchest\b', re.I),           'Chest'),
            (re.compile(r'\bthigh', re.I),             'Thigh'),
            (re.compile(r'\bshoulder', re.I),          'Shoulder'),
            (re.compile(r'\bback\b', re.I),            'Back'),
            (re.compile(r'\bleg\b', re.I),             'Leg'),
            (re.compile(r'\blegs\b', re.I),            'Leg'),
            (re.compile(r'\bcalf\b', re.I),            'Calf'),
            (re.compile(r'\bcalves\b', re.I),          'Calf'),
            (re.compile(r'\bankle', re.I),             'Ankle'),
            (re.compile(r'\bwrist', re.I),             'Wrist'),
            (re.compile(r'\bhip\b', re.I),             'Hip'),
            (re.compile(r'\bhips\b', re.I),            'Hip'),
            (re.compile(r'\babdomen', re.I),           'Abdomen'),
            (re.compile(r'\babdominal', re.I),         'Abdomen'),
            (re.compile(r'\bstomach\b', re.I),         'Abdomen'),
            (re.compile(r'\bbelly\b', re.I),           'Abdomen'),
            (re.compile(r'\brib\b', re.I),             'Ribcage'),
            (re.compile(r'\bribs\b', re.I),            'Ribcage'),
            (re.compile(r'\bribcage\b', re.I),         'Ribcage'),
            (re.compile(r'\bcollarbone\b', re.I),      'Collarbone'),
            (re.compile(r'\bclavicle\b', re.I),        'Collarbone'),
            (re.compile(r'\bneck\b', re.I),            'Neck'),
            (re.compile(r'\btorso\b', re.I),           'Torso'),
            (re.compile(r'\bspine\b', re.I),           'Spine'),
            (re.compile(r'\belbow\b', re.I),           'Elbow'),
            (re.compile(r'\bwaist\b', re.I),           'Waist'),
            (re.compile(r'\bnavel\b', re.I),           'Navel'),
            (re.compile(r'\bface\b', re.I),            'Face'),
            (re.compile(r'\bfinger', re.I),            'Finger'),
            (re.compile(r'\bhand\b', re.I),            'Hand'),
            (re.compile(r'\bhands\b', re.I),           'Hand'),
            (re.compile(r'\bfoot\b', re.I),            'Foot'),
            (re.compile(r'\bfeet\b', re.I),            'Foot'),
            (re.compile(r'\bknee\b', re.I),            'Knee'),
            (re.compile(r'\bside\b', re.I),            'Side'),
            (re.compile(r'\bbreast\b', re.I),          'Breast'),
            (re.compile(r'\bbreasts\b', re.I),         'Breast'),
            (re.compile(r'\bbuttock', re.I),           'Buttock'),
            (re.compile(r'\bbutt\b', re.I),            'Buttock'),
            (re.compile(r'\bglute', re.I),             'Buttock'),
        ]

        def _normalize_tattoo(kw: str):
            """If kw is a tattoo keyword referencing a body location, return the
            canonical 'Tattoo - Location' string.  Returns None for design-only
            tattoo keywords (e.g. 'flower tattoo') so they pass through normally."""
            k = kw.lower()
            if 'tattoo' not in k and 'tattooed' not in k:
                return None
            for pattern, location in _TATTOO_LOCATIONS:
                if pattern.search(k):
                    return f'Tattoo - {location}'
            return None  # design tattoo — no location detected

        # Nudity-level normalizer — maps the many ways a model can describe
        # the subject's state of dress/undress to a small set of canonical tags.
        # The patterns are tested in order (most-specific first).
        _NUDITY_RULES = [
            # Fully nude / naked
            (re.compile(r'\b(fully\s+nude|completely\s+nude|fully\s+naked|completely\s+naked|'
                        r'total\s+nudity|full\s+nudity|entirely\s+naked|entirely\s+nude)\b', re.I),
             'Nude'),
            (re.compile(r'\b(nude|naked|undressed|unclothed|bare\s+body|full\s+frontal)\b', re.I),
             'Nude'),
            # Topless (has bottom coverage)
            (re.compile(r'\b(topless|bare\s+chested|bare\s+breasted|shirtless)\b', re.I),
             'Topless'),
            # Bottomless (has top coverage)
            (re.compile(r'\b(bottomless|bare\s+below\s+the\s+waist)\b', re.I),
             'Bottomless'),
        ]

        def _normalize_nudity(kw: str):
            """Map nudity-level descriptions to canonical tags.
            Returns the canonical tag string, or None if the keyword doesn't
            describe a nudity level."""
            k = kw.lower().strip()
            for pattern, canonical in _NUDITY_RULES:
                if pattern.search(k):
                    return canonical
            return None

        # Pubic hair normalizer — maps many phrasings to a small set of
        # canonical descriptors.  Ordered specific → general.
        # Handles both "pubic" variants ("shaved pubic hair", "trimmed pubes")
        # and "pussy" variants ("shaved pussy", "trimmed pussy") since both
        # are common LLM output forms.
        _PUBIC_HAIR_RULES = [
            (re.compile(r'\b(shaved?\s+(pub(ic|es|is)|pussy)|bare\s+(pub(ic|es|is)|pussy)|'
                        r'clean[\s-]shaved?\s+(pub|gen|pussy)|hairless\s+pub)', re.I),
             'Shaved Pussy'),
            (re.compile(r'\b(landing\s+strip|racing\s+stripe?)\b', re.I),
             'Landing Strip'),
            (re.compile(r'\b(full\s+bush|full\s+pub|unshaved?\s+(pub|pussy)|'
                        r'unshaved?\s+gen|hairy\s+(pub|pussy))\b', re.I),
             'Full Bush'),
            (re.compile(r'\b(natural\s+(pub|pussy))\b', re.I),
             'Natural Pubic Hair'),
            (re.compile(r'\b(trimmed?\s+(pub|pussy|gen)|neat(ly)?\s+trim|'
                        r'trimmed?\s+hair.{0,15}pub|cropped?\s+(pub|pussy))\b', re.I),
             'Trimmed Pussy'),
            # Generic "pubic hair" with no qualifier — leave for alias lookup
        ]

        def _normalize_pubic_hair(kw: str):
            """Map pubic-hair grooming descriptions to canonical tags.
            Returns a canonical string or None."""
            k = kw.lower().strip()
            # Must mention pubic area, genitals, pussy, or explicit pubic hair context
            if not re.search(r'\b(pub(ic|es|is)|genital|vulva|vagina|labia|crotch|pussy)\b', k):
                return None
            for pattern, canonical in _PUBIC_HAIR_RULES:
                if pattern.search(k):
                    return canonical
            return None

        # Labia/vulva appearance normalizer — maps descriptive phrasings to stable
        # canonical forms so they have a consistent alias to match in the DB.
        # Ordered specific → general.  Only fires when labia/pussy/vagina appears.
        _LABIA_RULES = [
            (re.compile(r'\b(gaping|wide[\s-]+open)\s+(pussy|vagina|labia)\b', re.I),
             'Gaping Pussy'),
            (re.compile(r'\b(spread|open|parted|exposed|apart)\s+labia\b', re.I),
             'Spread Labia'),
            (re.compile(r'\blabia\s+(spread|open|parted|exposed|apart)\b', re.I),
             'Spread Labia'),
            (re.compile(r'\b(spread|open|parted|exposed)\s+(pussy|vagina)\b', re.I),
             'Spread Pussy'),
            # Bare "labia" mention without state qualifier — pass to tag_matcher as-is
        ]

        def _normalize_labia(kw: str):
            """Normalise labia/vulva appearance descriptions to canonical forms.
            Returns a canonical string or None.  Falls through to tag_matcher for
            plain 'labia' so the DB alias can provide the canonical form."""
            k = kw.lower().strip()
            if not re.search(r'\b(labia|pussy|vagina|vulva)\b', k):
                return None
            for pattern, canonical in _LABIA_RULES:
                if pattern.search(k):
                    return canonical
            return None

        # Color words that should be stripped from object/clothing keywords.
        # Anatomy tags (hair, skin, nipples) keep their color — colour IS the tag there.
        # True color words — strip these when they prefix object/clothing keywords.
        # Do NOT include materials (lace, sheer, mesh) or patterns (floral, striped)
        # even if they can loosely imply a color — they carry their own meaning.
        _COLOR_WORDS = frozenset({
            'black', 'white', 'red', 'blue', 'green', 'yellow', 'pink', 'purple',
            'orange', 'brown', 'grey', 'gray', 'silver', 'gold', 'golden', 'navy',
            'beige', 'cream', 'tan', 'crimson', 'cyan', 'magenta', 'violet', 'teal',
            'maroon', 'olive', 'coral', 'peach', 'lavender', 'turquoise', 'khaki',
            'burgundy', 'scarlet', 'amber', 'nude',
        })
        # Color-shade modifiers — only strip these when followed by another word
        # (the loop already ensures we keep the last word, so they are safe).
        # 'soft' is intentionally excluded: "soft areolas" describes texture, not shade.
        _COLOR_MODS = frozenset({'light', 'dark', 'bright', 'pale', 'deep', 'hot',
                                 'neon', 'pastel', 'baby', 'dusty', 'warm'})
        # Material/texture prefix words that add no tagging value when prefixing
        # furniture or object terms ("wooden headboard" → "headboard").
        _MATERIAL_WORDS = frozenset({
            'wooden', 'wood', 'rattan', 'wicker', 'woven', 'bamboo',
            'glass', 'metal', 'metallic', 'iron', 'steel', 'chrome',
            'brass', 'copper', 'marble', 'stone', 'concrete', 'brick',
            'velvet', 'fabric', 'upholstered', 'rustic', 'vintage',
        })
        # Last words that indicate the color IS the relevant attribute — never strip.
        # Hair-coloring terms (highlights, roots, tips, ends) keep their color prefix
        # because "purple highlights" / "dark roots" / "blue tips" describe dye color.
        _COLOR_KEEP_LAST = frozenset({
            'hair', 'skin', 'nipple', 'nipples',
            'highlights', 'roots', 'tips', 'ends',
        })

        def _strip_colors(kw: str) -> str:
            """Strip leading color adjectives and material prefixes from object/
            clothing/furniture keywords.  Returns the stripped keyword, or the
            original if nothing was removed or if the last word is an anatomy
            color carrier ('hair', 'skin', 'nipple', 'highlights', …)."""
            words = kw.split()
            if not words or words[-1].lower() in _COLOR_KEEP_LAST:
                return kw
            i = 0
            while i < len(words) - 1:  # always keep at least the last word
                w = words[i].lower()
                if w in _COLOR_WORDS or w in _COLOR_MODS or w in _MATERIAL_WORDS:
                    i += 1
                else:
                    break
            return ' '.join(words[i:]) if i > 0 else kw

        def _resolve(keyword):
            """Return the keyword to keep, or None to discard.
            Tries tag_matcher (tags_export.json) first, then tag_matcher_fallback
            (stashdb_tags.json). Logs matched entries with their source; logs
            unmatched entries only when both sources fail.
            """
            # Drop keywords that describe absent attributes ("no X", "not X", "without X")
            if _NEGATIVE_RE.match(keyword.strip()):
                return None

            # Drop keywords that express uncertainty ("possibly X", "appears to be X")
            if _UNCERTAIN_RE.match(keyword.strip()):
                return None

            # Pass content-level classification tags through without alias lookup.
            # The tag instruction emits exactly one of "sfw", "nudity", "explicit"
            # per image; these are valid tags but may not exist in the tag vocabulary.
            if keyword.strip().lower() in _CONTENT_LEVEL_TAGS:
                return keyword.strip().lower()

            # Normalize multi-colour hair keywords ("blue-purple hair", "purple blue hair"
            # → "blue and purple hair").
            hair_normalized = _normalize_hair(keyword)
            if hair_normalized:
                keyword = hair_normalized

            # Map unnatural-colored hair to "Colored Hair" / "Multicolored Hair".
            # Runs after _normalize_hair so multi-colour forms are already canonical.
            colored_hair_tag = _resolve_colored_hair(keyword)
            if colored_hair_tag:
                if blacklist and any(b in colored_hair_tag.lower() for b in blacklist):
                    return _BLACKLISTED
                return colored_hair_tag

            # Normalize piercing keywords to "Piercing - Location" form.
            piercing_normalized = _normalize_piercing(keyword)
            if piercing_normalized:
                keyword = piercing_normalized

            # Normalize tattoo location keywords to canonical "Tattoo - Location" form.
            # This replaces the keyword entirely — the normalised form is then matched
            # against the tag file (and colour-stripping is skipped for tattoo terms).
            tattoo_normalized = _normalize_tattoo(keyword)
            if tattoo_normalized:
                keyword = tattoo_normalized

            # Normalize nudity-level keywords to canonical forms ("Nude", "Topless", …).
            nudity_normalized = _normalize_nudity(keyword)
            if nudity_normalized:
                if blacklist and any(b in nudity_normalized.lower() for b in blacklist):
                    return _BLACKLISTED
                return nudity_normalized

            # Normalize pubic-hair grooming descriptions to canonical forms
            # ("Shaved Pussy", "Trimmed Pussy", "Full Bush", "Landing Strip").
            pubic_normalized = _normalize_pubic_hair(keyword)
            if pubic_normalized:
                if blacklist and any(b in pubic_normalized.lower() for b in blacklist):
                    return _BLACKLISTED
                return pubic_normalized

            # Normalize labia/vulva appearance descriptions ("spread labia",
            # "gaping pussy") to canonical forms before alias lookup.
            labia_normalized = _normalize_labia(keyword)
            if labia_normalized:
                if blacklist and any(b in labia_normalized.lower() for b in blacklist):
                    return _BLACKLISTED
                return labia_normalized

            primary_active = self.tag_matcher.enabled
            fallback_active = self.tag_matcher_fallback.enabled
            if primary_active or fallback_active:
                if len(keyword.split()) > _MAX_MATCH_WORDS:
                    self.tag_matcher.log_unmatched(keyword)
                    return None
                # Try original keyword first; if no match, retry with colors stripped.
                # This lets explicit tag-file entries like "black stockings" still win.
                stripped = _strip_colors(keyword)
                candidates = [keyword] if stripped == keyword else [keyword, stripped]
                result = None
                source = self.tag_matcher
                for candidate in candidates:
                    result = self.tag_matcher.match(candidate) if primary_active else None
                    if result:
                        source = self.tag_matcher
                        break
                    if fallback_active:
                        result = self.tag_matcher_fallback.match(candidate)
                        if result:
                            source = self.tag_matcher_fallback
                            break
                if result:
                    tag, alias = result
                    if blacklist and any(b in tag.lower() for b in blacklist):
                        return _BLACKLISTED
                    source.log_matched(keyword, alias, tag)
                    return tag
                self.tag_matcher.log_unmatched(keyword)
                return None
            return normalize_keyword(keyword, self.banned_words, self.config)

        if self.config.update_keywords:
            existing_keywords = metadata.get("MWG:Keywords", [])

            if isinstance(existing_keywords, str):
                existing_keywords = [k.strip() for k in existing_keywords.split(",")]

            for keyword in existing_keywords:
                resolved = _resolve(keyword)
                if resolved and resolved is not _BLACKLISTED:
                    all_keywords.add(resolved)

        debug_map = {}
        for keyword in new_keywords:
            resolved = _resolve(keyword)
            debug_map[keyword] = resolved
            if resolved and resolved is not _BLACKLISTED:
                all_keywords.add(resolved)

        # Caption-based pattern extraction: scan the LLM's description text with
        # the normalizer patterns to catch tags that were implicit in the caption
        # but not emitted as discrete keywords.  Each sentence is checked
        # individually; sentences containing negations are skipped to avoid
        # false positives like "she is not nude".
        _caption = (metadata.get('MWG:Description') or '').strip()
        if _caption:
            _SENT_SPLIT_RE = re.compile(r'[.!?]+')
            _NEG_SENT_RE   = re.compile(
                r'\b(not|no|without|isn\'t|aren\'t|doesn\'t|don\'t|never)\b', re.I
            )
            for _sent in _SENT_SPLIT_RE.split(_caption):
                _sent = _sent.strip()
                if not _sent or _NEG_SENT_RE.search(_sent):
                    continue
                for _fn in (_normalize_nudity, _normalize_pubic_hair, _normalize_labia):
                    _canon = _fn(_sent)
                    if _canon and not (blacklist and any(b in _canon.lower() for b in blacklist)):
                        all_keywords.add(_canon)

        result = list(all_keywords)
        if return_debug:
            return result, debug_map
        return result
        
def main(config=None, callback=None, check_paused_or_stopped=None):
    if config is None:
        config = Config.from_args()
    
    if not hasattr(config, 'chunk_size'):
        config.chunk_size = 100

    # Resume: pre-load skip-set for non-DB mode from the checkpoint file.
    # DB-mode skip-set is loaded inside FileProcessor after the connection opens.
    skip_paths = None
    if getattr(config, 'resume_session', False):
        output_mode = getattr(config, 'output_mode', 'json')
        if output_mode not in ('db', 'both'):
            cp = _checkpoint_path()
            if cp.exists():
                try:
                    data = json.loads(cp.read_text(encoding='utf-8'))
                    if os.path.normpath(data.get('directory', '')) == os.path.normpath(str(config.directory)):
                        paths = data.get('processed_paths', [])
                        skip_paths = {os.path.normpath(p) for p in paths}
                        msg = f"Resume: {len(skip_paths):,} previously-processed files will be skipped."
                        print(msg)
                        if callback:
                            callback(msg)
                    else:
                        print("Checkpoint directory mismatch — starting fresh.")
                        if callback:
                            callback("Warning: checkpoint is for a different directory; starting fresh.")
                except Exception as e:
                    print(f"Warning: checkpoint load failed: {e}")

    file_processor = FileProcessor(
        config, check_paused_or_stopped, callback, skip_paths=skip_paths
    )      
    
    try:
        file_processor.process_directory(config.directory)
    
    except KeyboardInterrupt:
        print("Processing interrupted. State saved for resuming later.")
        
        if callback:
            callback("Processing interrupted. State saved for resuming later.")
    
    except Exception as e:
        print(f"Error occurred during processing: {str(e)}")
    
        if callback:
            callback(f"Error: {str(e)}")
            
    finally:
        print("Waiting for indexer to complete...")
        file_processor.indexer.join()
        print("Indexing completed.")
        
   
if __name__ == "__main__":
    main()
