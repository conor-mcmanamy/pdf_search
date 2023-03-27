""" Outlines pdf search """
import glob
import os
import re
from datetime import datetime
from pathlib import Path

import PyPDF2 as pdf
import typer
from joblib import Parallel, delayed

""" Simple CLI tool to query sets of PDFs for search terms """


app = typer.Typer()


@app.command()
def gather_input_files(pdf_dir: str = "."):
    """Compile manifest of pdfs to search """
    pdfs = glob.glob(f"{pdf_dir}{os.path.sep}*.pdf")
    print(f"Found {len(pdfs)} pdfs in {pdf_dir=}")
    return pdfs


def define_search():
    """Returns patterns of search"""
    with open(Path("patterns") / "patterns.txt", "r", encoding="utf-8") as f:
        patterns = f.read().splitlines()
    print(f"Search defined: {patterns=}")
    return "|".join(patterns)


@app.command()
def find_in_pdf(pattern: str, pdf_path: str) -> list:
    """searches for `pattern` in `pdf_path`"""
    try:
        matches = []
        with open(pdf_path, "rb") as f:
            pdf_rdr = pdf.PdfReader(f)
            for p_num in range(len(pdf_rdr.pages)):
                txt = pdf_rdr.pages[p_num].extract_text()
                if isinstance(txt, str) and txt != "":
                    if re.search(pattern, txt, re.I):
                        print(f"Match on {pdf_path},{p_num=}")
                        matches.append(p_num)
    except Exception:
        return (matches, pdf_path)
    else:
        return (matches, None)


@app.command()
def main(
    pdf_dir: str = typer.Argument("input"),
    run_parallel: bool = typer.Option(False, help="Parallelize search using joblib"),
):
    """Runs search on directory of PDFs"""
    TIMESTAMP = datetime.now().strftime("%Y.%m.%d_%H_%M")
    print(f"Started search at {TIMESTAMP=}")

    files = gather_input_files(pdf_dir)
    if len(files) == 0:
        typer.Exit()

    pattern = define_search()

    if run_parallel:
        outcome = Parallel(n_jobs=-1, verbose=10)(
            delayed(find_in_pdf)(pattern, pdf) for pdf in files
        )
    else:
        outcome = [find_in_pdf(pattern, pdf) for pdf in files]

    match_pages, status = zip(*outcome)

    with open(f"matches_{TIMESTAMP}.txt", "w", encoding="utf-8") as f:
        for file, match_page_list in zip(files, match_pages):
            for match_page in match_page_list:
                print(f"{file}\t{match_page}", file=f)

    failures = list(filter(None, status))
    if len(failures) > 0:
        for fail in failures:
            print(f"Failure to search {fail}")


if __name__ == "__main__":
    app()
