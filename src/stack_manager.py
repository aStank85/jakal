# src/stack_manager.py

from typing import Dict, List, Optional, Any
from src.database import Database
from src.calculator import MetricsCalculator


class StackManager:
    """CRUD operations for stacks and stack membership."""

    def __init__(self, db: Database):
        self.db = db
        self.calculator = MetricsCalculator()

    # --- Stack CRUD ---

    def create_stack(self, name: str, description: str = None) -> int:
        """Create a named stack. Returns stack_id."""
        existing = self.db.get_stack_by_name(name)
        if existing:
            raise ValueError(f"Stack '{name}' already exists")
        return self.db.create_stack(name, stack_type='named', description=description)

    def get_stack(self, stack_id: int) -> Optional[Dict]:
        """Get a stack by ID."""
        return self.db.get_stack(stack_id)

    def get_stack_by_name(self, name: str) -> Optional[Dict]:
        """Get a stack by name."""
        return self.db.get_stack_by_name(name)

    def get_all_stacks(self) -> List[Dict]:
        """Get all stacks."""
        return self.db.get_all_stacks()

    def update_stack(self, stack_id: int, name: str = None, description: str = None) -> None:
        """Update a stack's name and/or description."""
        stack = self.db.get_stack(stack_id)
        if not stack:
            raise ValueError(f"Stack {stack_id} not found")
        if name is not None:
            existing = self.db.get_stack_by_name(name)
            if existing and existing['stack_id'] != stack_id:
                raise ValueError(f"Stack '{name}' already exists")
        self.db.update_stack(stack_id, name=name, description=description)

    def delete_stack(self, stack_id: int) -> None:
        """Delete a stack and all its members."""
        stack = self.db.get_stack(stack_id)
        if not stack:
            raise ValueError(f"Stack {stack_id} not found")
        self.db.delete_stack(stack_id)

    # --- Member management ---

    def add_player_to_stack(self, stack_id: int, username: str, role_override: str = None) -> None:
        """Add a player to a stack by username."""
        stack = self.db.get_stack(stack_id)
        if not stack:
            raise ValueError(f"Stack {stack_id} not found")

        player_id = self.db.get_player_id(username)
        if player_id is None:
            raise ValueError(f"Player '{username}' not found in database")

        try:
            self.db.add_member_to_stack(stack_id, player_id, role_override)
        except Exception:
            raise ValueError(f"Player '{username}' is already in this stack")

    def remove_player_from_stack(self, stack_id: int, username: str) -> None:
        """Remove a player from a stack by username."""
        player_id = self.db.get_player_id(username)
        if player_id is None:
            raise ValueError(f"Player '{username}' not found in database")
        self.db.remove_member_from_stack(stack_id, player_id)

    def get_stack_members(self, stack_id: int) -> List[Dict]:
        """Get all members of a stack."""
        return self.db.get_stack_members(stack_id)

    def get_stack_size(self, stack_id: int) -> int:
        """Get number of players in a stack."""
        return self.db.get_stack_size(stack_id)

    # --- Stack type operations ---

    def create_quick_stack(self, usernames: List[str]) -> int:
        """Create a temporary quick stack from a list of usernames."""
        if len(usernames) < 2:
            raise ValueError("Quick stack needs at least 2 players")

        stack_id = self.db.create_stack(
            name=f"Quick Stack ({len(usernames)} players)",
            stack_type='quick',
            description='Temporary stack for quick analysis'
        )

        for username in usernames:
            player_id = self.db.get_player_id(username)
            if player_id is None:
                self.db.delete_stack(stack_id)
                raise ValueError(f"Player '{username}' not found in database")
            self.db.add_member_to_stack(stack_id, player_id)

        return stack_id

    def build_tagged_stack(self, tag: str = 'teammate') -> int:
        """Build a stack from all players with a matching tag.

        Note: Player tagging is not yet implemented in v0.3,
        so this builds from all players in the database as a placeholder.
        Future versions will filter by tag.
        """
        all_players = self.db.get_all_players()
        usernames = [
            p["username"] if isinstance(p, dict) else p
            for p in all_players
        ]
        if len(usernames) < 2:
            raise ValueError(f"Not enough players to build tagged stack (found {len(usernames)})")

        # Delete any previous tagged stack with same tag
        for stack in self.db.get_all_stacks():
            if stack['stack_type'] == 'tagged' and tag in (stack['description'] or ''):
                self.db.delete_stack(stack['stack_id'])

        stack_id = self.db.create_stack(
            name=f"Tagged: {tag}",
            stack_type='tagged',
            description=f"Auto-built from tag: {tag}"
        )

        for username in usernames:
            player_id = self.db.get_player_id(username)
            if player_id is not None:
                self.db.add_member_to_stack(stack_id, player_id)

        return stack_id

    def validate_stack(self, stack_id: int) -> Dict[str, Any]:
        """Validate a stack - check members, stats availability."""
        stack = self.db.get_stack(stack_id)
        if not stack:
            return {'valid': False, 'size': 0, 'missing_players': [], 'warnings': ['Stack not found']}

        members = self.db.get_stack_members(stack_id)
        size = len(members)
        missing_players = []
        warnings = []

        for member in members:
            snapshot = self.db.get_latest_snapshot(member['username'])
            if snapshot is None:
                missing_players.append(member['username'])

        if size == 0:
            warnings.append("Stack is empty")
        elif size < 5:
            warnings.append(f"Only {size} players in stack (5 recommended)")
        elif size > 5:
            warnings.append(f"Stack has {size} players (5 is standard)")

        if missing_players:
            warnings.append(f"{len(missing_players)} player(s) have no stats: {', '.join(missing_players)}")

        valid = size >= 2 and len(missing_players) == 0
        return {
            'valid': valid,
            'size': size,
            'missing_players': missing_players,
            'warnings': warnings
        }

    def cleanup_quick_stacks(self) -> None:
        """Delete all quick (temporary) stacks."""
        self.db.delete_stacks_by_type('quick')
