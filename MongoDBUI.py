import csv
from tkinter import *
import customtkinter
import mongoDBNew
import webbrowser
import os

# from lbnlp.models.load.matscholar_2020v1 import load

# ner_model = load("ner")

customtkinter.set_appearance_mode("light")
customtkinter.set_default_color_theme("blue")
root_tk = customtkinter.CTk()
root_tk.geometry(f"{1200}x{800}")
root_tk.title("MongoDB Search")
root_tk.resizable(0, 0)

# root_tk.rowconfigure(2, weight=2)

background_color = "#E2E9EA"
text_color = "#000000"
searchFrame = customtkinter.CTkFrame(root_tk, width=1200, height=150)
searchFrame.pack(side=TOP)

# tableFrame = customtkinter.CTkFrame(root_tk, width=1200, height=650)
# tableFrame.pack(side=BOTTOM)

# tableFrame.grid(column=1, row=1)


searchDatabase = "All"
value = ""
searchType = "on"
searchParam = "Header and Value"
searchFlag = "Both"
curPage = 0


def flatten(tags):
    flattened_tags = []
    for sentence in tags:
        flattened_tags.extend(sentence)
    return flattened_tags


def generate_csv():
    os.startfile(mongoDBNew.csvOutput() + '.csv')


# Use CTkButton instead of tkinter Button
button = customtkinter.CTkButton(master=searchFrame, text="Generate CSV", command=generate_csv)

button.place(relx=0.56, rely=0.25, anchor=W)

entry = customtkinter.CTkEntry(master=searchFrame,
                               placeholder_text="Search",
                               width=300,
                               height=40,
                               border_width=2,
                               corner_radius=5,
                               text_font=("Gill Sans", 14))
entry.place(relx=0.53, rely=0.25, anchor=E)


def database_callback(choice):
    global searchDatabase
    searchDatabase = choice


database_choice = customtkinter.CTkOptionMenu(master=searchFrame,
                                              values=["Test", "Melt Pool", "Refractory Alloys", "Super Alloys",
                                                      "Mechanical Properties", "Hea Creep", "Super Alloys Creep",
                                                      "All"],
                                              command=database_callback)
database_choice.place(relx=0.29, rely=0.7, anchor=CENTER)
database_choice.set("All")  # set initial value


def search_callback(choice):
    global searchParam
    searchParam = choice


search_choice = customtkinter.CTkOptionMenu(master=searchFrame,
                                            values=["Table header", "Value", "Header and Value", "PDF Title",
                                                    "Table Title", "Tagged Title"],
                                            command=search_callback)
search_choice.place(relx=0.43, rely=0.7, anchor=CENTER)
search_choice.set("Header and Value")  # set initial value

def flag_callback(choice):
    global searchFlag
    searchFlag = choice


flag_choice = customtkinter.CTkOptionMenu(master=searchFrame,
                                          values=["Non Flagged", "Flagged", "Both"],
                                          command=flag_callback)
flag_choice.place(relx=0.57, rely=0.7, anchor=CENTER)
flag_choice.set("Both")  # set initial value

check_var = StringVar(value="on")

def checkbox_event():
    global searchType
    searchType = check_var.get()


checkbox = customtkinter.CTkCheckBox(master=searchFrame, text="Part of Word", command=checkbox_event,
                                     variable=check_var, onvalue="on", offvalue="off")
checkbox.place(relx=0.69, rely=0.7, anchor=CENTER)

def switch_event(data_row, switch):
    mongoDBNew.update_db(data_row['id'], "Flag", switch.get() == "on")


def openPDF_event(data_row):
    webbrowser.open(data_row['doi'], new=0, autoraise=True)


def editPDF_event(data_row):
    os.remove('temp.csv')
    create_popup(data_row)
    with open("temp.csv", "w+", newline='') as my_csv:
        csv_writer = csv.writer(my_csv)
        csv_writer.writerow(['Table title', data_row['title']])
        csv_writer.writerows(data_row['table_csv'])
    os.startfile('temp.csv')


def create_popup(data_row):
    window = customtkinter.CTkToplevel()
    window.geometry("350x70")
    window.attributes('-topmost', True)
    # create label on CTkToplevel window
    update = customtkinter.CTkButton(master=window, text="Update",
                                     command=lambda a=data_row, b=window: updateTable_event(a, b))
    update.place(relx=0.25, rely=0.5, anchor=CENTER)
    cancel = customtkinter.CTkButton(master=window, text="Cancel", command=lambda a=window, b=window: cancel_update(a))
    cancel.place(relx=0.75, rely=0.5, anchor=CENTER)


def updateTable_event(data_row, window):
    csv_reader = csv.reader(open('temp.csv', 'r'))
    rows = []

    for row in csv_reader:
        rows.append(row)
    title = rows[0][1]
    row_num = 0
    header = rows[1]
    values = rows[2:]
    tables = []
    # flattened_tags = flatten(ner_model.tag_doc(title))
    tagged_title = ["Not Updated"]
    tag = ["Not Updated"]
    # for word in flattened_tags:
    #     if word[1] != 'O':
    #         tagged_title.append(word[0])
    #         tag.append(word[1])
    for row in values:
        table = []
        for col in range(len(header)):
            table.append({'name': header[col], 'value': row[col]})
            # print(header[col] + ',' + values[row_num][col])
        tables.append({'body': table, 'title': title, 'tagged_title': tagged_title, 'tags': tag,
                       'pdf_title': data_row['pdf_title'], 'Flag': False})
    mongoDBNew.edit_table(data_row['id'], tables)
    search
    cancel_update(window)


def cancel_update(window):
    # shutil.rmtree('C:/Users/Jason/Documents/PDFExtractionProject/TableExtraction-master/temp.csv')
    window.destroy()


def nextPage(data):
    global curPage
    if len(data) >= 25*(curPage+1):
        curPage += 1
        clearFrame()
        populate(data)

def prevPage(data):
    global curPage

    if curPage-1 >= 0:
        curPage -= 1
        clearFrame()
        populate(data)


def populate(data):
    global curPage
    global outputFrame

    lastRow = 0
    totalRows = 0

    '''Put in some fake data'''
    for row in range(curPage * 25, curPage * 25 + 25):
        # print(row)
        totalRows += 1
        if row >= len(data):
            break
        data_row = data[row]
        if searchFlag == 'Both' or (searchFlag == 'Flagged' and data_row['flagged']) or (
                searchFlag == 'Non Flagged' and not data_row['flagged']):
            # print(data_row['table'])
            Label(outputFrame, text="PDF Title: " + data_row['pdf_title'], font=('Courier', 12), bg=background_color,
                  fg=text_color,
                  justify=RIGHT).grid(
                row=6 * row, column=0, sticky='w')
            Label(outputFrame, text="Title: " + data_row['title'], font=('Courier', 12), bg=background_color,
                  fg=text_color,
                  justify=LEFT).grid(
                row=6 * row + 1, column=0, sticky='w')
            Label(outputFrame, text="Tagged Title: " + data_row['tagged_title'], font=('Courier', 12),
                  bg=background_color,
                  fg=text_color,
                  justify=LEFT).grid(
                row=6 * row + 2, column=0, sticky='w')
            Label(outputFrame, text=data_row['table'],
                  font=('Courier', 12), bg=background_color, fg=text_color, relief=GROOVE, justify=LEFT).grid(
                row=6 * row + 3, column=0, pady=20, sticky='w')

            flagged_initial = "on" if data_row['flagged'] else "off"
            switch_var = StringVar(value=flagged_initial)

            switch = customtkinter.CTkSwitch(outputFrame, text="Flagged", variable=switch_var,
                                             onvalue="on", offvalue="off")
            switch.grid(row=6 * row + 4, column=0, pady=20, sticky='w')
            switch.configure(command=lambda a=data_row, b=switch: switch_event(a, b))

            customtkinter.CTkButton(outputFrame, text="Open PDF", command=lambda a=data_row: openPDF_event(a)).grid(
                row=6 * row + 4, column=0, padx=100,
                sticky='w')
            customtkinter.CTkButton(outputFrame, text="Edit", command=lambda a=data_row: editPDF_event(a)).grid(
                row=6 * row + 4, column=0, padx=260,
                sticky='w')

            Label(outputFrame, text="\n_____________________________________________________________\n",
                  font=('FuraMono NF', 12), bg=background_color, fg=text_color, justify=LEFT).grid(
                row=6 * row + 5, column=0, sticky='w')
        lastRow = row
    customtkinter.CTkButton(outputFrame, text="<=", command=lambda a=data: prevPage(a)).grid(
        row=6*(lastRow + 1), column=0, padx=0,
        sticky='W')

    customtkinter.CTkButton(outputFrame, text="=>", command=lambda a=data: nextPage(a)).grid(
        row=6*(lastRow + 1), column=0, padx=150,
        sticky='w')

    Label(outputFrame, text="Page: "+str(curPage+1),
          font=('Courier', 12), bg=background_color, fg=text_color, justify=LEFT).grid(
        row=6*(lastRow + 1), column=0, padx=300, sticky='w')



def onFrameConfigure(canvas):
    '''Reset the scroll region to encompass the inner frame'''
    canvas.configure(scrollregion=canvas.bbox("all"))


def onMousewheel(event):
    outputCanvas.yview_scroll(int(-1 * (event.delta / 120)), "units")


outputCanvas = Canvas(root_tk, width=1200, borderwidth=0, background=background_color)
outputFrame = customtkinter.CTkFrame(outputCanvas, fg_color=background_color)
vsb = Scrollbar(root_tk, orient="vertical", command=outputCanvas.yview)
hsb = Scrollbar(root_tk, orient="horizontal", command=outputCanvas.xview)

outputCanvas.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

hsb.pack(side="bottom", fill="x")
vsb.pack(side="right", fill="y")
outputCanvas.pack(side=LEFT, fill="both", expand=True)
outputCanvas.create_window((5, 2), window=outputFrame, anchor="nw")

outputFrame.bind("<Configure>", lambda event, canvas=outputCanvas: onFrameConfigure(canvas))
root_tk.bind("<MouseWheel>", onMousewheel)


# populate(outputFrame)


#
# vbar.config(command=outputArea.yview)
# vbar.pack(side=RIGHT, fill=Y)
#
# hbar = Scrollbar(tableFrame, orient=HORIZONTAL, command=outputArea.xview)
# hbar.pack(side=BOTTOM, fill=X)


def search(event):
    global value
    global curPage

    value = entry.get()
    # print(value + "," + searchType + "," + searchDatabase + "," + searchParam)
    clearFrame()
    curPage = 0
    # populate(outputFrame, mongoDB.search(searchParam, searchType, searchDatabase, value))
    # print(searchDatabase)

    # Contains all the data/tables
    output_list = mongoDBNew.search(searchParam, searchType, searchDatabase, value)
    populate(output_list)


def clearFrame():
    global outputFrame
    # destroy all widgets from frame

    for widget in outputFrame.winfo_children():
        widget.destroy()


root_tk.bind('<Return>', search)

root_tk.mainloop()

# class PDFElem:
#     pdfTitle = ""
#     title = ""
#     taggedTitle = ""
#     table = ""
#     flagged = False
#
#     def __init__(self, data_row):
#         self.pdfTitle = data_row['pdf_title']
#         self.title = data_row['title']
#         self.taggedTitle = data_row['tagged_title']
#         self.table = data_row['table']
#         self.flagged = data_row['flagged']
