<<<<<<< HEAD
# jakal
=======
# Jakal - Rainbow Six Siege Stats Analyzer

Jakal is a terminal-based stats analyzer for Rainbow Six Siege that helps you track, analyze, and compare player performance over time.

## Features

### Core Functionality
- **Stats Import**: Copy-paste stats directly from R6 Tracker season drawer
- **Historical Tracking**: Store multiple snapshots per player across different dates/seasons
- **Advanced Metrics**: Calculate derived stats like entry efficiency, clutch success rates, teamplay index
- **Role Detection**: Automatically identify player roles (Fragger, Entry, Support, Anchor, Clutch, Carry)
- **Player Comparison**: Compare multiple players side-by-side across key stats

### Tracked Statistics
- **Game Stats**: Matches, wins/losses, win %, time played, abandons
- **Round Stats**: Rounds played, round win %, disconnects
- **Combat Stats**: K/D, kills/deaths per round, assists, headshot %, first bloods/deaths
- **Clutch Stats**: 1v1 through 1v5 success rates, clutch attempts
- **Multikills**: Aces, 4K, 3K, 2K tracking
- **Ranked Stats**: Current/max rank, rank points, TRN Elo

### Derived Metrics
- **Entry Efficiency**: First blood success rate
- **Aggression Score**: Frequency of entry attempts
- **Clutch Metrics**: Attempt rate, 1v1 success, disadvantaged clutch success
- **Teamplay Index**: Ratio of assists to total eliminations
- **Role Scores**: Fragger, Entry, Support, Anchor, Clutch Specialist, Carry

## Requirements

- Python 3.7 or higher
- No external dependencies (uses Python standard library only)

## Installation

1. Clone or download this repository
2. Navigate to the project directory:
   ```bash
   cd jakal-mvp
   ```

3. Run the application:
   ```bash
   python main.py
   ```

## Usage

### Adding Player Stats

1. Go to [R6 Tracker](https://r6.tracker.network/)
2. Search for a player
3. Navigate to their season stats drawer
4. Copy the entire stats section
5. In Jakal, select option 1 "Add new stats snapshot"
6. Paste the stats when prompted
7. Type `END` on a new line
8. Enter player metadata (username, date, season)

### Viewing Players

Select option 2 to see all players in your database.

### Comparing Players

1. Select option 3 "Compare players"
2. Enter player numbers separated by commas (e.g., `1,3,5`)
3. View side-by-side comparison with:
   - Individual stat values
   - Winner for each category
   - Overall advantages count

### Example Comparison Output
```
Players:
  1. Player1 (Fragger) - 2024-02-12
  2. Player2 (Support) - 2024-02-12

Stat                      P1          P2          Winner
K/D                       1.38        1.15        P1
Win %                     52.10       48.30       P1
HS %                      60.00       55.20       P1
Entry Efficiency          0.63        0.52        P1
1v1 Clutch %             0.73        0.68        P1
```

## Project Structure

```
jakal-mvp/
├── data/
│   └── jakal.db              # SQLite database
├── src/
│   ├── parser.py             # R6 Tracker stats parser
│   ├── database.py           # Database operations
│   ├── calculator.py         # Metrics calculation
│   ├── comparator.py         # Player comparison logic
│   └── ui.py                 # Terminal UI
├── tests/
│   ├── test_parser.py        # Parser tests
│   └── test_calculator.py    # Calculator tests
├── docs/
│   └── v0.1-notes.md         # Version notes
├── main.py                   # Application entry point
├── requirements.txt          # Dependencies
└── README.md                 # This file
```

## How It Works

### 1. Parsing
The parser (`src/parser.py`) uses a state machine to parse copy-pasted R6 Tracker stats:
- Identifies section headers (Game, Rounds, Combat, etc.)
- Extracts stat name/value pairs
- Normalizes stat names to consistent keys
- Handles percentages, times, and comma-separated numbers
- Validates critical stats are present

### 2. Storage
Stats are stored in SQLite (`data/jakal.db`) with three tables:
- **players**: Player records
- **stats_snapshots**: Raw stats for each snapshot
- **computed_metrics**: Derived metrics and role classifications

### 3. Calculation
The calculator (`src/calculator.py`) computes advanced metrics:
- Entry efficiency = first bloods / (first bloods + first deaths)
- Clutch success rates for different scenarios
- Teamplay index = assists / (assists + kills)
- Role scores using weighted formulas
- Role classification with confidence levels

### 4. Comparison
The comparator (`src/comparator.py`) provides head-to-head analysis:
- Extracts comparable stats from snapshots
- Determines winners for each metric
- Calculates overall advantages

## Database Schema

### Players Table
- `player_id`: Primary key
- `username`: Unique player identifier
- `created_at`: Timestamp

### Stats Snapshots Table
- Links to player via `player_id`
- Stores all raw stats from R6 Tracker
- Includes snapshot metadata (date, time, season)
- Clutch data stored as JSON for flexibility

### Computed Metrics Table
- Links to snapshot via `snapshot_id`
- Stores all derived metrics
- Includes role classification results

## Future Enhancements

- [ ] Player progression tracking (compare snapshots over time)
- [ ] Export comparisons to CSV/JSON
- [ ] Visualization with graphs and charts
- [ ] Web interface
- [ ] Operator-specific stats tracking
- [ ] Map performance analysis
- [ ] Team composition recommendations
- [ ] Import from R6 Tracker API (if available)

## Version History

### v0.1 (Current)
- Initial MVP release
- Core stats parsing and storage
- Advanced metrics calculation
- Role identification system
- Multi-player comparison
- Terminal UI

## Contributing

This is currently a personal project, but suggestions and feedback are welcome!

## License

This project is for personal use. Rainbow Six Siege and R6 Tracker are properties of their respective owners.
>>>>>>> 7d94de4 (Initial Commit - JAKAL v0.2.0)
