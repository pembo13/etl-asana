from .helper import initialize_tables, get_milestone, upsert_milestone

initialize_tables()

__all__ = ['upsert_milestone', 'get_milestone']
