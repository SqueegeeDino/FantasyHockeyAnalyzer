import streamlit as st

name = st.text_input("Insert text here")
if st.button("Greet"):
    st.write(f"Hello {name}")