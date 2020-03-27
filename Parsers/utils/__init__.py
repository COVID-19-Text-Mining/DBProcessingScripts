def clean_title(title):
    title = title.split("Running Title")[0]
    title = title.replace("^Running Title: ", "")
    title = title.replace("^Short Title: ", "")
    title = title.replace("^Title: ", "")
    title = title.strip()
    return title
