package utils

func Merge[T any](first, second []T, less func(a, b T) bool) []T {
	result := make([]T, len(first)+len(second))

	i, j, k := 0, 0, 0

	for i < len(first) && j < len(second) {
		if less(first[i], second[j]) {
			result[k] = first[i]
			i++
		} else {
			result[k] = second[j]
			j++
		}
		k++
	}

	for i < len(first) {
		result[k] = first[i]
		i++
		k++
	}

	for j < len(second) {
		result[k] = second[j]
		j++
		k++
	}

	return result
}
