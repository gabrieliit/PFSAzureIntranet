from reportlab.pdfgen import canvas
import os
from reportlab.lib.pagesizes import letter
from reportlab.platypus import  Table,TableStyle,Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
import locale

class Document:
    def __init__(self,name,folder,size=letter,locale_param="en_IN"):
        self.name=name #name of the pdf
        self.folder=folder #path where it is stored
        self.path= os.path.join(folder, name)
        self.c = canvas.Canvas(self.path, pagesize=size)
        self.pg_width, self.pg_height = size
        self.pages=[] #list of pages. Each page is a page object
        locale.setlocale(locale.LC_ALL,locale_param)
    def add_page(self,n_sections=1):
        new_page=Page(self.c,self.pg_width, self.pg_height,n_sections)
        self.pages.append(new_page)
        return new_page
    def render_doc(self):
        cur_pg=1
        for page in self.pages:
            page.render_sections() #render the sections of the page
            if cur_pg<=len(self.pages): #if not last page
                self.c.showPage()  #add new page
                cur_pg+=1  #update cur page
        self.c.save()

class Page:
    def __init__(self,canvas,width, height,n_sections):
        self.n_secs = n_sections  # number of sections in the page
        self.width, self.height = width, height
        self.left_indent = self.width / 10
        self.top_indent = self.height / 10
        self.c=canvas
        self.sections=[]
        self.section_height=(self.height-self.top_indent)/self.n_secs
    def add_section(self,type,heading,heading_style="Helvetica-Bold",heading_size=12,content=None,content_font="Helvetica-Bold",content_font_size=10,**kwargs):
        #add a section to a page using the factory function in the Doc_Section class
        left_start_pos=self.left_indent
        top_start_pos=self.height-self.top_indent-len(self.sections)*self.section_height
        section_width=self.width-2*self.left_indent
        try:
            section_height_scaling_factor=kwargs['shsf']
        except KeyError:
            section_height_scaling_factor=1.0
        try:
            section_width_scaling_factor = kwargs['swsf']
        except KeyError:
            section_width_scaling_factor = 1.0
        section=Doc_section.create_section(type,self.c,section_width*section_width_scaling_factor,left_start_pos, top_start_pos,self.section_height*section_height_scaling_factor, heading,heading_style,heading_size,content,content_font,content_font_size,**kwargs)
        #def create_section(self,type,canvas,left_start_pos, top_start_pos,section_height,heading,heading_style="Helvetica-Bold",heading_size=12,content=None,content_font="Helvetica-Bold",content_font_size=10,**kwargs)
        self.sections.append(section)
    def render_sections(self):
        for section in self.sections:
                section.render()

class Doc_section:
    def __init__(self,type,canvas,section_width,left_start_pos, top_start_pos,section_height,heading,heading_style="Helvetica-Bold",font_size=12,content=None):
        self.type=type #type can be heading, table or image
        self.heading=heading
        self.heading_style=heading_style
        self.content=content
        self.font_size=font_size
        self.canvas=canvas
        self.left_start_pos=left_start_pos
        self.top_start_pos=top_start_pos
        self.section_height=section_height
        self.section_width=section_width
    def render(self):
        pass#this will be implemented by child classes
    @classmethod
    def create_section(cls,type,canvas,section_width,left_start_pos, top_start_pos,section_height,heading,heading_style="Helvetica-Bold",heading_size=12,content=None,content_font="Helvetica-Bold",content_font_size=8,**kwargs):
        'factory function to create the correct child class'
        if type == 'table':
            try:
                row_label=kwargs["row_label"]
                table_height_sf=kwargs["table_height_sf"]
            except KeyError:
                row_label="" #no row label specified for tabel. Leave it blank
                table_height_sf=1.1
            table=Doc_section_table(canvas,section_width,left_start_pos, top_start_pos,section_height,heading,heading_style,heading_size,content,content_font,content_font_size,row_label,table_height_sf)
            return table
        elif type=='image':
            image= Doc_section_image(canvas,section_width,left_start_pos, top_start_pos,section_height,heading,heading_style,heading_size,content,content_font,content_font_size)
            return image
            #_init__(self,canvas,left_start_pos, top_start_pos,section_height,heading_text,heading_style,font_size,content, table_font_size, table_font,row_label)
class Doc_section_heading(Doc_section):
    def __init__(self,canvas,section_width,left_start_pos, top_start_pos,section_height,heading_text,heading_style,font_size,content):
        super().__init__('heading',canvas,section_width,left_start_pos, top_start_pos,section_height,heading_text, heading_style,font_size,content)

class Doc_section_table(Doc_section):
    def __init__(self,canvas,section_width,left_start_pos, top_start_pos,section_height,heading_text,heading_style,font_size,content, table_font, table_font_size,row_label,table_height_sf=1.1):
        super().__init__('table', canvas,section_width,left_start_pos, top_start_pos,section_height,heading_text, heading_style,font_size,content)
        self.table_font_size=table_font_size
        self.table_font=table_font
        self.row_label=row_label
        self.table_height_sf=table_height_sf
        #add error checkign to ensure content is of type df
    def render(self):
        # Print section heading
        heading_bottom=self.top_start_pos-self.font_size
        self.canvas.drawString(self.left_start_pos,heading_bottom,self.heading)
        # Add summary statistics as a table
        table_cols = [[self.row_label] + list(self.content.columns)]
        for index, row in self.content.iterrows():
            formatted_row = []
            for value in row:
                try:
                    formatted_row.append(locale.format_string('%.2f', value, grouping=True))
                except TypeError: #Non numeric value
                    formatted_row.append(str(value))
            table_cols.append([index] + formatted_row)
        styles = getSampleStyleSheet()
        styleN = styles['Normal']
        styleN.wordWrap = 'CJK'
        styleN.alignment=0
        styleN.fontSize=self.table_font_size
        table_data = [[Paragraph(cell, styleN) for cell in row] for row in table_cols]

        # Create the table
        row_height=self.table_font_size*1.5
        table = Table(table_data, colWidths=self.section_width/(len(self.content.columns)+1))
        # Define the table style
        style = TableStyle([('WORDWRAP', (0, 0), (-1, -1), 'WORDWRAP')])
        # Apply the table style
        table.setStyle(style)
        self.canvas.setFont(self.table_font, self.table_font_size)
        # Add cell borders manually
        for i in range(len(table_data)):
            for j in range(len(table_data[0])):
                table.setStyle([
                    ('GRID', (j, i), (j, i), 1, colors.black),
                    ('ALIGN', (j, i), (j, i), 'LEFT'),
                ])
        # Calculate the vertical position for the table
        table_height = len(table_data) * row_height*1.5
        table_bottom = heading_bottom - 2*self.font_size-table_height*self.table_height_sf  # Leave a blank line between Heading and table top row

        # Draw the table on the canvas
        table.wrapOn(self.canvas, self.section_width, table_height)
        table.drawOn(self.canvas, self.left_start_pos, table_bottom)

class Doc_section_image(Doc_section):
    def __init__(self,canvas,section_width,left_start_pos, top_start_pos,section_height,heading_text,heading_style,font_size,image_path, image_font, image_font_size):
        super().__init__('image',canvas,section_width,left_start_pos, top_start_pos,section_height,heading_text, heading_style,font_size,image_path)
        self.image_font_size=image_font_size
        self.image_font=image_font
    def render(self):
        # Print section heading
        heading_bottom=self.top_start_pos-2*self.font_size
        self.canvas.setFont(self.heading_style,self.font_size)
        self.canvas.drawString(self.left_start_pos,heading_bottom,self.heading)
        # Print image
        self.canvas.setFont(self.image_font, self.image_font_size)
        fig_start_vert=heading_bottom-self.image_font_size#leave a line between geadign and fig start
        fig_bottom=self.top_start_pos-self.section_height
        self.canvas.drawInlineImage(self.content, self.left_start_pos, fig_bottom, width=self.section_width, height=(fig_start_vert-fig_bottom)*0.95)
