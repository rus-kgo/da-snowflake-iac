import textwrap


def box_message(message: str, title: str = "", width: int = 80):
    """
    Prints a message inside a Unicode box with a title centered in the top border.
    """
    # Box drawing characters
    top_left, top_right = "╭", "╮"
    bottom_left, bottom_right = "╰", "╯"
    horizontal = "─"

    # 1. Split message into lines and handle auto-width
    lines = message.strip().splitlines()

    # 1. Prepare the Top Line with Title
    if title:
        title_text = f" {title} "
        # Calculate how many dashes on each side
        side_dashes = (width - len(title_text)) // 2
        top_line = (
            top_left
            + (horizontal * side_dashes)
            + title_text
            + (horizontal * (width - len(title_text) - side_dashes))
            + top_right
        )
    else:
        top_line = top_left + (horizontal * width) + top_right

    middle_section = []
    for line in lines:
        # We use .center() to keep the formatting you liked
        # Use .ljust() instead of .center() if you want SQL to be left-aligned
        content = line.ljust(width)
        wrapped_content = textwrap.fill(
            content,
            width=width,
            initial_indent="  ",
            subsequent_indent="  ",
            drop_whitespace=True,
        )
        middle_section.append(f"{wrapped_content}")

    middle_lines = "\n".join(middle_section)

    # 3. Prepare the Bottom Line
    bottom_line = bottom_left + (horizontal * width) + bottom_right

    return f"\n{top_line}\n{middle_lines}\n{bottom_line}\n"


if __name__ == "__main__":
    print(
        box_message(""" CREATE
   OR
   ALTER SCHEMA MODELING.MODELING_SCHEMA DATA_RETENTION_TIME_IN_DAYS = NONE ;ddddasdasdasdasdasdasdasdsadasdasdasddddddddddddddddddddd
   ALTER TABLE PRESENTATION.EXAMPLE_SCHEMA.SECOND_TABLE ALTER COLUMN COLUMN1 SET DATA TYPE NUMBER;
   ALTER TABLE PRESENTATION.EXAMPLE_SCHEMA.SECOND_TABLE ALTER COLUMN COLUMN1 DROP NOT NULL;
   """)
    )
