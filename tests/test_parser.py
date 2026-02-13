# test_parser.py

from src.parser import R6TrackerParser, pretty_print_stats

def main():
    """Simple test script to try the parser."""
    
    print("="*60)
    print("R6 TRACKER PARSER TEST")
    print("="*60)
    print("\nPaste your R6 Tracker stats below.")
    print("When finished, type 'END' on a new line and press Enter.\n")
    
    # Collect input
    lines = []
    while True:
        try:
            line = input()
            if line.strip().upper() == 'END':
                break
            lines.append(line)
        except EOFError:
            break
    
    pasted_text = '\n'.join(lines)
    
    # Parse
    parser = R6TrackerParser()
    
    try:
        stats = parser.parse(pasted_text)
        pretty_print_stats(stats)
        
        print("\n✅ Parsing successful!")
        print("\nQuick Stats:")
        print(f"  K/D: {stats['combat']['kd']}")
        print(f"  Win Rate: {stats['game']['match_win_pct']}%")
        print(f"  Headshot %: {stats['combat']['hs_pct']}%")
        print(f"  Matches: {stats['game']['matches']}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()