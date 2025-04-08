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
search = input("\nWhat keyword to search: ")
search_words = [search]

for search_word in search_words:
        df2 = df.copy()
        df2.drop(["Time", "Date"], axis="columns", inplace=True)

        df2["First_7_Words"] = df2["Message"].str.split().str[:7].str.join(" ")

        df2["flag"] = df2["First_7_Words"].str.contains(fr"{search_word}", case=False, regex=True)

        df3 = df2[df2["flag"] == True].drop(["First_7_Words"], axis="columns")

        # Transforming data for adaptability to system
        df3.drop(["flag"], axis="columns", inplace=True)

        print(f"Count: {df3.shape[0]}")
df3.to_csv(f"{search_word}.csv", index=False)
        