from typing import Optional


def remove_output_node(
    output: str, node: str, start: Optional[int] = None, end: Optional[int] = None
) -> str:
    """Remove a GraphQL Output node from the output string.

    Parameters
    ----------
    output : str
        GraphQL output string
    node : str
        Node identifier
    start : Optional[int], optional
        index to start node search useful for nodes sharing a name, by default None
    end : Optional[int], optional
        index to end node search useful for nodes sharing a name, by default None

    Returns
    -------
    str
        Modified output string
    """
    try:
        start_idx = output.index(f" {node} ", start, end) + 1
    except ValueError:
        return output

    end_idx = None

    brackets_found = 0
    for idx, char in enumerate(output):
        if idx < start_idx + len(node):
            continue

        if char == " ":
            continue

        if brackets_found == 0 and char != "{":
            end_idx = idx
            break

        if char == "{":
            brackets_found += 1

        elif char == "}":
            brackets_found -= 1

        end_idx = idx + 1

    return output[:start_idx] + output[end_idx:]
