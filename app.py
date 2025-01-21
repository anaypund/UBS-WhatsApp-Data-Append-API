import re
import pandas as pd


txt_path = input("Path of WhatsApp chat data (txt): ")
# Load WhatsApp chat data from a text file
with open(txt_path, "r", encoding="utf-8") as file:
    raw_data = file.readlines()

# Define the regex pattern to parse date, time, sender, and message
pattern = r"^(\d{1,2}/\d{1,2}/\d{2}),\s*(\d{1,2}:\d{2})\s*-\s*(.*?):\s*(.*)"

# Initialize a list to store extracted messages
data = []

# Temporary variables for multi-line message handling
current_message = ""
current_date = ""
current_time = ""
current_name = ""

# Process each line in the chat file
for line in raw_data:
    match = re.match(pattern, line)
    if match:
        # If line matches the pattern, it's a new message
        if current_message:
            # Save the previous message if exists
            data.append([current_date, current_time, current_name, current_message.strip()])
        
        # Extract components for the new message
        current_date = match.group(1)
        current_time = match.group(2)
        current_name = match.group(3)
        current_message = match.group(4)
    else:
        # If line does not match, assume it's a continuation of the previous message
        current_message += " " + line.strip()

# Save the last message if it exists
if current_message:
    data.append([current_date, current_time, current_name, current_message.strip()])

# Convert the data into a pandas DataFrame
df = pd.DataFrame(data, columns=["Date", "Time", "Name", "Message"])

# Save to a CSV file or display the DataFrame
# df.to_csv("whatsapp_chat_parsed.csv", index=False)


df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%y')

# Define the start date and end date (adjust as needed)
start_date = input("Start Date (DD/MM/YY): ")
end_date = input("End Date (DD/MM/YY): ")

# Convert the start and end dates to datetime
start_date = pd.to_datetime(start_date, format='%d/%m/%y')
end_date = pd.to_datetime(end_date, format='%d/%m/%y')
df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

# Read from the contact db csv
contact_df = pd.read_csv("Contact pairs.csv")
contact_df.sample(3)

contact_dict = {}
for i in range(len(contact_df)):
    contact_dict.update({contact_df.iloc[i, 0]: contact_df.iloc[i, 1]})

df['Name'] = df['Name'].apply(lambda x: x if x not in contact_dict else contact_dict[x])

## Classify data for LDI, SD and Gratitude or SD Pitch

# csv_path = input("Enter path to CSV file: ")
search = int(input("\nChoose what data to push:\n1. LDI/SD/Gratitudes\n2. SD Pitch:\n (1 or 2): "))
sundayDate = input("\nEnter the end date of week which will be shown (DD/MM/YYYY): ")
if search == 1:
    search_words = ["SD", "LDI", "gratitude"]
elif search == 2:
    search_words = ["SD"]

for search_word in search_words:
    if search_word == "SD":
        df2 = df.copy()
        df2.drop(["Time", "Date"], axis="columns", inplace=True)

        df2["First_7_Words"] = df2["Message"].str.split().str[:7].str.join(" ")

        df2["flag"] = df2["First_7_Words"].str.contains(fr"\b{search_word}\b", case=False, regex=True)

        df3 = df2[df2["flag"] == True].drop(["Message", "First_7_Words"], axis="columns")

        # Transforming data for adaptability to system
        df3.drop(["flag"], axis="columns", inplace=True)
        df3['flag'] = "Yes"
        df3['Date'] = sundayDate

        print(f"Count: {df3.shape[0]}")
        
        if search == 1:
            sd_df = df3.copy()
            df3.to_csv(f'{search_word}.csv', header=False, index=False)
        else:
            sd_pitch_df = df3.copy()
            df3.to_csv(f'{search_word} Pitch.csv', header=False, index=False)
    elif search_word == "LDI":
        df2 = df.copy()
        df2.drop(["Time", "Date"], axis="columns", inplace=True)

        df2["First_7_Words"] = df2["Message"].str.split().str[:7].str.join(" ")

        df2["flag"] = df2["First_7_Words"].str.contains(fr"\b{search_word}\b", case=False, regex=True)

        df3 = df2[df2["flag"] == True].drop(["Message", "First_7_Words"], axis="columns")
        
        # Transforming data for adaptability to system
        df3.drop(["flag"], axis="columns", inplace=True)
        df3['flag'] = "Yes"
        df3['Date'] = sundayDate

        print(f"Count: {df3.shape[0]}")

        ldi_df = df3.copy()
        df3.to_csv(f'{search_word}.csv', header=False, index=False)
    elif search_word == "gratitude":
        df2 = df.copy()
        df2.drop(["Time", "Date"], axis="columns", inplace=True)

        df2["First_7_Words"] = df2["Message"].str.split().str[:7].str.join(" ")

        df2['Date'] = sundayDate
        df2["flag"] = df2["First_7_Words"].str.contains(fr"\b{search_word}\b", case=False, regex=True)

        df3 = df2[df2["flag"] == True].drop(["Message", "First_7_Words"], axis="columns")

        # Transforming data for adaptability to system
        df3.drop(["flag"], axis="columns", inplace=True)
        df3['flag'] = "TRUE"

        print(f"Count: {df3.shape[0]}")

        gratitude_df = df3.copy()
        df3.to_csv(f'{search_word}.csv', header=False, index=False)


import gspread
from gspread_dataframe import set_with_dataframe

gc = gspread.service_account(filename='ubs-whatsapp-data-push-c191e1cd21aa.json')
spreadsheet = gc.open("UBS | Backend")

def append_to_sheets(dataframes, subsheet_names):
    # Append each DataFrame to the corresponding subsheet
    for df, subsheet_name in zip(dataframes, subsheet_names):
        worksheet = spreadsheet.worksheet(subsheet_name)

        # Find the next available row in column B
        existing_data = worksheet.get_all_values()
        next_row = len(existing_data) + 1

        # Append DataFrame below existing data in column B
        set_with_dataframe(worksheet, df, row=next_row, col=2, include_index=False, include_column_header=False)
        print(f'{df.shape[0]} rows appended to "{subsheet_name}" sheet from row number {next_row}!')

    print("All Data Appended successfully!")
    print("Thank You")


if search_words == ["SD"]:
    append_to_sheets([sd_pitch_df], ["SD Pitch"])
else:
    append_to_sheets([gratitude_df, ldi_df, sd_df], ["Gratitudes", "LDI Reflections", "SD Reflections"])