ANNOTATION_SEPARATOR = ','


class BSkyCounter:
    def __init__(self) -> None:
        self._count: dict[str, int] = {}
        self._count_since_last_access: dict[str, int] = {}

    def increment(self, annotation: str) -> None:
        if annotation not in self._count:
            self._count[annotation] = 1
            self._count_since_last_access[annotation] = 1
        else:
            self._count[annotation] += 1
            self._count_since_last_access[annotation] += 1

    def topk(self, k: int = 10, reset_incremental_access: bool = True
             ) -> list[tuple[str, int]]:
        result_since_last_access: list[tuple[str, int]] = sorted(
            self._count_since_last_access.items(), key=lambda x: x[1],
            reverse=True
        )[:k]

        if reset_incremental_access:
            for annotation in self._count_since_last_access.keys():
                self._count_since_last_access[annotation] = 0

        # We want actual counts instead of incremental counts
        result: list[tuple[str, int]] = [
            (annotation, self._count[annotation])
            for annotation, _ in result_since_last_access
        ]

        return result
