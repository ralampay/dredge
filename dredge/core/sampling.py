"""Bounded deterministic object sampling, preferring held-out objects."""
import hashlib


def sample_objects(selection, discovered, limit, *, fallback=False):
    held_out, reused = [], []
    matched = eligible = 0
    for obj in selection.objects():
        matched += 1
        was_sampled = obj.key in discovered
        if not was_sampled:
            eligible += 1
        if was_sampled and not fallback:
            continue
        candidates = reused if was_sampled else held_out
        candidates.append((hashlib.sha256(('validate:' + obj.key).encode()).hexdigest(), obj))
        candidates.sort(key=lambda item: item[0])
        del candidates[limit:]
    selected = held_out + reused[:max(0, limit - len(held_out))]
    return [obj for _, obj in selected], matched, eligible


class SelectedObjects:
    """Restrict a record reader to sampled objects, retaining partition semantics."""
    def __init__(self, selection, objects):
        self.selection, self.selected = selection, objects

    def objects(self):
        return iter(self.selected)

    def partition_values(self, obj):
        return self.selection.partition_values(obj)
