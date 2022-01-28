def get_tuple(list_for_tuple):
    result = tuple(list_for_tuple) if len(list_for_tuple)>1 else f"('{list_for_tuple[0]}')"
    return result
