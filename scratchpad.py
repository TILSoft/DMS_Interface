# %%
#%%
df = get_po_qty(
    "select quantiteof as qty, uniteof as unit from elan2406PRD.xfp_ofentete where numof = '0920978216'")
df

#%%
