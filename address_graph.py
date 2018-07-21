from igraph import Graph, plot,Plot

from igraph.drawing.text import TextDrawer
import pandas as pd
import pickle
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from math import log
import cairo
import math
import py2neo

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def query_database(query):
    # REMEMBER TO BE CONNECTED TO IMPERIAL WIFI!
    graph_db = py2neo.Graph("https://dsi-bitcoin.doc.ic.ac.uk:7473/db/data/", auth=("guest_ro", "imperialO_nly"))
    return graph_db.run(query)

def get_block_data(first_block, last_block):
    query_string = """
                    MATCH (b:Block) <-[:MINED_IN]- (t:Tx) <-[:IN]- (txi:TxIn) <-[:UNLOCK]- (iadr:Address)
                    WHERE b.height >= {} AND b.height <= {}
                    MATCH (txi) <-[:SPENT]- (txo_in:TxOut)
                    MATCH (oadr:Address) <-[:LOCK]- (txo_out:TxOut) <-[:OUT]- (t)

                    RETURN iadr.address as iadr, oadr.address as oadr, txo_in.value as input_val, txo_out.value as output_val, ID(txo_in) as id_txo_in, ID(txi) as id_txi, ID(t) as id_t, ID(txo_out) as id_txo_out
                    """.format(first_block, last_block)
    return query_string

def address_graph(result):
    tups1 = []
    for d in result:
        tups1.append((d['iadr'],d['oadr']))
    import math

    ig = Graph.TupleList(tups1,directed=True)

    layout = ig.layout_kamada_kawai()
    visual_style = {}
    visual_style["layout"] = layout
    visual_style["bbox"]= (10000, 10000)
    visual_style["margin"] = 50
    visual_style["autocurve"] = True
    visual_style["arrow_size"] = 0.01

    #visual_style["vertex_label"] = ig.vs['label']
    #visual_style['edge_width'] = [0.03*i for i in ig.es['weight']]
    #visual_style['edge_color'] = [color[i] for i in ig.es['platform']]
    visual_style['keep_aspect_ratio'] = True

    size = []
    for i in ig.degree():
        if i > 1:
            size.append(20*log(i))
        else:
            size.append(i)
    visual_style["vertex_size"] = size

    p = Plot("addr_graph.png", bbox=(10000, 10000), background="white")
    p.add(ig, **visual_style)
    # p.redraw()

    fileName = 'graph2.png'
    context = cairo.Context(p.surface)
    context.set_font_size(60)
    title = "Address Graph of Block 400000"
    drawer = TextDrawer(context, text=title, halign=TextDrawer.CENTER)
    drawer.draw_at(x=1745, y=100, width=600)
    p.save(fileName)
    ig.savegml('filename')


result = query_database(get_block_data(400000,400000))
address_graph(result)
