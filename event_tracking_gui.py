from Tkinter import *
import string
import ast
import time

global scrollbar

class App:
    def __init__(self, master):
        frame = Frame(master)
        frame.pack()
        
        self.hi_there = Button(frame, text="Draw", command= lambda: self.say_hi(1))
        self.hi_there.pack(side=TOP)
        #self.scrollbar = Scrollbar(frame,orient=HORIZONTAL)
        #self.scrollbar.pack(fill=Y, expand=FALSE)

     
    def say_hi(self,sel):
		def show_event(event,val):
			val = "Event -> "+val
			self.rect_obj.create_text(500,700,text=val,tags="hovertext")
		
		def remove_event(event):
			htext = self.rect_obj.find_withtag("hovertext")
			time.sleep(0.2)
			self.rect_obj.delete(htext)
			
		
		if sel==1:
			self.hi_there.config(relief=SUNKEN)
			self.rect_obj = Canvas(self.hi_there.master, width=2000, height=2000)
			self.rect_obj.pack()
			spacing = 20
			f = open('nodes.txt','r')
			map_sig = {}
			lines = [line.strip() for line in f]
			d=ast.literal_eval(lines[0])

			for i in range(1,len(d)+1):
				sigs=d[i-1]
				self.rect_obj.create_text((spacing*i)+(i*10),0+spacing,text=str(i))
				for j in range(1,len(sigs)+1):
					tag_str = 't'+str(i+1)+str(j+1)
					map_sig[sigs[j-1]]=tag_str
					self.rect_obj.create_oval((spacing*i)+(i*10), (spacing*j)+(j*10), (spacing*i)+(i*10)+10, (spacing*j)+(j*10)+10, tags=tag_str,activefill='#000000')
					node = self.rect_obj.find_withtag(tag_str)
					self.rect_obj.tag_bind(node, '<Enter>',lambda event, arg=sigs[j-1]: show_event(event,arg)) 
					self.rect_obj.tag_bind(node, '<Leave>', remove_event)
			f.close()
			
			f = open('edges.txt','r')
			lines = [line.strip() for line in f]
			d=ast.literal_eval(lines[0])
			for i in range(0,len(d)):
				edge=d[i]
				sig1 = map_sig[edge[0]]
				sig2 = map_sig[edge[1]]
				t1 = self.rect_obj.find_withtag(sig1)
				t2 = self.rect_obj.find_withtag(sig2)
				coords1 = self.rect_obj.coords(t1)
				coords2 = self.rect_obj.coords(t2)
				x1 = (coords1[0]+coords1[2])/2
				y1 = (coords1[1]+coords1[3])/2
				x2 = (coords2[0]+coords2[2])/2
				y2 = (coords2[1]+coords2[3])/2
				self.rect_obj.create_line(x1,y1,x2,y2)
			f.close()

		elif sel==2:
			self.hi_there.config(relief=RAISED)
    

			
root = Tk()
app = App(root)
root.mainloop()
