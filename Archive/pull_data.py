import requests
import time
import pandas as pd
import json
import os
from bs4 import BeautifulSoup
import re

# ---- STEP 1: FETCH ALL QUESTIONS ----


def fetch_nlp_questions(page=1, pagesize=100, has_accepted=True):
    """
    Fetch questions tagged with 'nlp', optionally filtering for those with accepted answers.

    Args:
        page: The page number to fetch
        pagesize: Number of questions per page
        has_accepted: If True, only fetch questions with accepted answers

    Returns:
        tuple: (questions_list, quota_remaining)
    """
    base_url = "https://api.stackexchange.com/2.3/questions"

    # Build request parameters
    params = {
        "site": "stackoverflow",
        "page": page,
        "pagesize": pagesize,
        "order": "desc",
        "sort": "votes",
        "tagged": "nlp",
        "filter": "withbody",  # Standard filter that includes body
    }

    # Add hasaccepted parameter if requested
    if has_accepted:
        params["hasaccepted"] = "true"

    print(f"Fetching page {page} of NLP questions...")
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        questions = data.get("items", [])
        quota = data.get("quota_remaining", 0)

        print(f"Retrieved {len(questions)} questions (page {page})")
        print(f"API quota remaining: {quota}")

        # Extract only the fields we need to save memory
        processed_questions = []
        for q in questions:
            processed_questions.append(
                {
                    "question_id": q["question_id"],
                    "title": q.get("title", ""),
                    "body": q.get("body", ""),
                    "score": q.get("score", 0),
                    "creation_date": q.get("creation_date", 0),
                    "view_count": q.get("view_count", 0),
                    "answer_count": q.get("answer_count", 0),
                    "tags": ";".join(q.get("tags", [])),
                }
            )

        return processed_questions, quota
    else:
        print(f"Error fetching questions: {response.status_code}")
        return [], 0


def save_questions(questions, filename="nlp_questions.json"):
    """Save fetched questions to a JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(questions, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(questions)} questions to {filename}")


def load_questions(filename="nlp_questions.json"):
    """Load questions from a JSON file."""
    if not os.path.exists(filename):
        print(f"No questions file found at {filename}")
        return []

    with open(filename, "r", encoding="utf-8") as f:
        questions = json.load(f)
    print(f"Loaded {len(questions)} questions from {filename}")
    return questions


def collect_all_questions(
    max_pages=300,
    start_page=11,
    has_accepted=True,
    save_interval=5,
    output_file="nlp_questions.json",
):
    """
    Collect all NLP questions up to max_pages.

    Args:
        max_pages: Maximum number of pages to fetch
        start_page: Page to start from
        has_accepted: If True, only fetch questions with accepted answers
        save_interval: How often to save progress
        output_file: File to save questions to

    Returns:
        list: All collected questions
    """
    # Load existing questions if file exists
    all_questions = load_questions(output_file) if os.path.exists(output_file) else []

    # Track question IDs to avoid duplicates
    existing_ids = {q["question_id"] for q in all_questions}
    page = start_page

    try:
        for page_num in range(start_page, start_page + max_pages):
            # Fetch a page of questions
            questions, quota = fetch_nlp_questions(page_num, 100, has_accepted)

            # Add new questions (avoid duplicates)
            new_count = 0
            for q in questions:
                if q["question_id"] not in existing_ids:
                    all_questions.append(q)
                    existing_ids.add(q["question_id"])
                    new_count += 1

            print(f"Added {new_count} new questions (total: {len(all_questions)})")

            # Save at intervals
            if page_num % save_interval == 0:
                save_questions(all_questions, output_file)

            # Stop if quota is running low
            if quota < 5:
                print(f"API quota running low ({quota}). Stopping.")
                break

            # Stop if no questions found (likely end of results)
            if len(questions) == 0:
                print("No questions found on this page. May have reached the end.")
                # Check the next page to confirm
                next_questions, _ = fetch_nlp_questions(page_num + 1, 100, has_accepted)
                if len(next_questions) == 0:
                    print("Confirmed end of results (next page is also empty).")
                    break

            # Rate limiting
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
    except Exception as e:
        print(f"Error: {e}")

    # Final save
    save_questions(all_questions, output_file)
    return all_questions


# ---- STEP 2: FETCH ANSWERS FOR QUESTIONS ----


def clean_html(html_text):
    """Clean HTML content to plain text."""
    if not html_text:
        return ""

    try:
        soup = BeautifulSoup(html_text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        # Clean up excessive whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text
    except:
        # Fallback to simple regex
        text = re.sub(r"<[^>]+>", "", html_text)
        return re.sub(r"\s+", " ", text).strip()


def fetch_answers_for_questions(question_ids, top_n=3):
    """
    Fetch answers for a batch of question IDs.

    Args:
        question_ids: List of question IDs to fetch answers for
        top_n: Number of top non-accepted answers to include

    Returns:
        dict: Mapping of question ID to answer information
    """
    if not question_ids:
        return {}

    # Stack Exchange API limits: max 100 IDs per request
    batch_size = 100
    all_answers = {}

    for i in range(0, len(question_ids), batch_size):
        batch = question_ids[i : i + batch_size]
        ids_string = ";".join(map(str, batch))

        print(
            f"Fetching answers for {len(batch)} questions (batch {i // batch_size + 1})..."
        )

        base_url = f"https://api.stackexchange.com/2.3/questions/{ids_string}/answers"
        params = {
            "site": "stackoverflow",
            "order": "desc",
            "sort": "votes",
            "filter": "withbody",
            "pagesize": 100,
        }

        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            answers = data.get("items", [])
            quota = data.get("quota_remaining", 0)

            print(f"Retrieved {len(answers)} answers")
            print(f"API quota remaining: {quota}")

            # Group answers by question ID
            for answer in answers:
                qid = answer["question_id"]
                if qid not in all_answers:
                    all_answers[qid] = {"accepted": [], "others": []}

                # Process answer
                answer_data = {
                    "answer_id": answer["answer_id"],
                    "score": answer.get("score", 0),
                    "user_id": answer.get("owner", {}).get("user_id", "unknown"),
                    "text": clean_html(answer.get("body", "")),
                }

                # Add to appropriate list
                if answer.get("is_accepted", False):
                    all_answers[qid]["accepted"].append(answer_data)
                else:
                    all_answers[qid]["others"].append(answer_data)

            # Check quota
            if quota < 100:
                print(f"API quota running low ({quota}). Consider resuming later.")
                break

            # Rate limiting
            time.sleep(1)
        else:
            print(f"Error fetching answers: {response.status_code}")

    # Sort and limit other answers
    for qid in all_answers:
        # Sort by score (descending)
        all_answers[qid]["others"].sort(key=lambda x: x["score"], reverse=True)
        # Limit to top_n
        all_answers[qid]["others"] = all_answers[qid]["others"][:top_n]

    return all_answers


def process_questions_with_answers(
    questions_file="nlp_questions.json",
    output_file="nlp_qa_dataset.csv",
    batch_size=30,
    top_n=3,
):
    """
    Process questions and fetch their answers.

    Args:
        questions_file: JSON file containing questions
        output_file: CSV file to write results to
        batch_size: Number of questions to process in each batch
        top_n: Number of top non-accepted answers to include per question
    """
    # Load questions
    questions = load_questions(questions_file)
    if not questions:
        print("No questions to process.")
        return

    # Check for existing processed data
    processed_ids = set()
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            processed_ids = set(existing_df["question_id"].tolist())
            print(f"Found {len(processed_ids)} already processed questions")
        except Exception as e:
            print(f"Error loading existing file: {e}")

    # Filter questions that need processing
    to_process = [q for q in questions if q["question_id"] not in processed_ids]
    print(f"Processing {len(to_process)} questions in batches of {batch_size}")

    # Process in batches
    results = []
    for i in range(0, len(to_process), batch_size):
        batch = to_process[i : i + batch_size]
        print(
            f"\nProcessing batch {i // batch_size + 1}/{(len(to_process) + batch_size - 1) // batch_size}"
        )

        # Get question IDs for this batch
        question_ids = [q["question_id"] for q in batch]

        # Fetch answers for this batch
        answers_data = fetch_answers_for_questions(question_ids, top_n)

        # Combine questions with their answers
        for question in batch:
            qid = question["question_id"]

            # Clean question body
            clean_body = clean_html(question.get("body", ""))

            # Prepare row for CSV
            row = {
                "question_id": qid,
                "title": question.get("title", ""),
                "body": clean_body,
                "score": question.get("score", 0),
                "view_count": question.get("view_count", 0),
                "answer_count": question.get("answer_count", 0),
                "tags": question.get("tags", ""),
                "accepted_answer": "",
                "top_answer_1": "",
                "top_answer_2": "",
                "top_answer_3": "",
            }

            # Add answers if available
            if qid in answers_data:
                # Add accepted answer
                if answers_data[qid]["accepted"]:
                    accepted = answers_data[qid]["accepted"][
                        0
                    ]  # Take first if multiple
                    row["accepted_answer"] = (
                        f"[User {accepted['user_id']} | Score: {accepted['score']}]: {accepted['text']}"
                    )

                # Add top answers
                for idx, answer in enumerate(answers_data[qid]["others"][:3]):
                    col_name = f"top_answer_{idx + 1}"
                    row[col_name] = (
                        f"[User {answer['user_id']} | Score: {answer['score']}]: {answer['text']}"
                    )

            results.append(row)

        # Save progress
        if results:
            new_df = pd.DataFrame(results)
            if os.path.exists(output_file) and processed_ids:
                # Append new data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df.to_csv(output_file, index=False, encoding="utf-8")
                print(
                    f"Updated {output_file} with {len(new_df)} new records (total: {len(combined_df)})"
                )
            else:
                # Create new file
                new_df.to_csv(output_file, index=False, encoding="utf-8")
                print(f"Created {output_file} with {len(new_df)} records")

    print("\nProcessing complete!")


# ---- MAIN EXECUTION ----


def main():
    """Main execution function."""
    print("=== StackOverflow NLP Question & Answer Data Collector ===")

    # Step 1: Collect all questions first
    print("\n--- STEP 1: Collecting Questions ---")
    questions = collect_all_questions(
        max_pages=100,  # Adjust based on your needs
        has_accepted=True,  # Only get questions with accepted answers
        save_interval=2,  # Save every 2 pages
        output_file="nlp_questions.json",
    )

    # Step 2: Process answers separately
    print("\n--- STEP 2: Collecting Answers ---")
    process_questions_with_answers(
        questions_file="nlp_questions.json",
        output_file="nlp_qa_dataset.csv",
        batch_size=30,  # Process 30 questions at a time
        top_n=3,  # Get top 3 non-accepted answers
    )


# To run step 1 only (collect questions)
def step1_only():
    print("Collecting questions only...")
    collect_all_questions(
        start_page=28,
        max_pages=100,
        has_accepted=True,
        save_interval=2,
        output_file="nlp_questions.json",
    )


# To run step 2 only (process answers for already collected questions)
def step2_only():
    print("Processing answers for existing questions...")
    process_questions_with_answers(
        questions_file="nlp_questions.json",
        output_file="nlp_qa_dataset.csv",
        batch_size=30,
        top_n=3,
    )


if __name__ == "__main__":
    # Uncomment ONE of these lines:
    # main()            # Run both steps in sequence
    step1_only()  # Run only step 1 (collect questions)
    # step2_only()    # Run only step 2 (process answers)
