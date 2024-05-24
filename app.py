import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from PyPDF2 import PdfReader
from langchain.text_splitter import CharacterTextSplitter, RecursiveCharacterTextSplitter
from langchain_community.document_loaders import AmazonTextractPDFLoader
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain_community.chat_models.openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationalRetrievalChain
from htmlTemplates import css, bot_template, user_template

from utils.helpers import (
    get_table_csv_results,
    store_objectIn_s3,
    get_signed_s3_Object
    )

logo_image = 'cap_logo.png'

def get_pdf_text(pdf_docs):
    text = ""
    for pdf in pdf_docs:
        pdf_reader = PdfReader(pdf)
        for page in pdf_reader.pages:
            text += page.extract_text()
    return text

def get_text_from_textract(pdf_doc):
    loader = AmazonTextractPDFLoader(pdf_doc)
    return loader.load()

def convert_pdf_to_csv(pdf_file, output_name):
    table_csv = get_table_csv_results(pdf_file)
    output_file = f'{output_name}.csv'
    with open(output_file, 'wt') as fout:
        fout.write(table_csv)
    
    print('CSV OUTPUT FILE:', output_file)

def get_text_chunks(text):
    text_splitter = CharacterTextSplitter(
        separator="\n",
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len
    )
    chunks = text_splitter.split_text(text)
    return chunks
    
def get_vectorstore(text_chunks):
    embeddings = OpenAIEmbeddings()
    vectorstore = FAISS.from_texts(texts=text_chunks, embedding=embeddings)
    return vectorstore


def get_conversation_chain(vectorstore):
    llm = ChatOpenAI()
    memory = ConversationBufferMemory(
        memory_key='chat_history', return_messages=True)
    conversation_chain = ConversationalRetrievalChain.from_llm(
        llm=llm,
        retriever=vectorstore.as_retriever(),
        memory=memory
    )
    return conversation_chain

def handle_userinput(user_question):
    response = st.session_state.conversation({'question': user_question})
    st.session_state.chat_history = response['chat_history']

    for i, message in enumerate(st.session_state.chat_history):
        if i % 2 == 0:
            st.write(user_template.replace(
                "{{MSG}}", message.content), unsafe_allow_html=True)
        else:
            st.write(bot_template.replace(
                "{{MSG}}", message.content), unsafe_allow_html=True)

def main():
    load_dotenv()
    st.set_page_config(page_title="ChatBot",)
    st.write(css, unsafe_allow_html=True)

    if "conversation" not in st.session_state:
        st.session_state.conversation = None
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = None

    st.header("ChatBot")
    user_question = st.text_input("Please Ask any questions related your Invoices or Reciepts")
    if user_question:
        handle_userinput(user_question)

    with st.sidebar:
        st.sidebar.image(logo_image)
        st.subheader("Your documents")
        pdf_docs = st.file_uploader(
            "Upload your PDFs here and click on 'Process'", accept_multiple_files=True)
        if st.button("Process"):
            final_text = ""
        
            with st.spinner("Processing"):
                for pdf_file in pdf_docs:
                    f_text = ""
                    out_d = 'pdfs'
                    save_pat = Path(out_d, pdf_file.name)

                    with open(save_pat, mode='wb') as w:
                        w.write(pdf_file.getvalue())
                    
                    pdf_p = out_d + '/' + pdf_file.name

                    docs = get_text_from_textract(pdf_p)
                    for doc in docs:
                        f_text += doc.page_content
                    
                    final_text += f_text
                text_chunks = get_text_chunks(final_text)
                vectorstore = get_vectorstore(text_chunks)
                st.session_state.conversation = get_conversation_chain(
                    vectorstore)
                
        if st.button("Convert PDF to CSV file"):
            if not pdf_docs:
                st.warning("Please upload some PDFs first.")
            else:
                with st.spinner("Converting PDFs to CSV..."):
                    for pdf_file in pdf_docs:
                        out_dir = 'pdfs'
                        save_path = Path(out_dir, pdf_file.name)
                        
                        with open(save_path, mode='wb') as w:
                            w.write(pdf_file.getvalue())
    
                        pdf_path = out_dir + '/' + pdf_file.name
                        store_objectIn_s3(pdf_path, pdf_file.name)
                        s3_path = get_signed_s3_Object(pdf_file.name)
                        
                        convert_pdf_to_csv(pdf_path, pdf_file.name)
                        print(s3_path)
                        os.remove(f'{out_dir}/{pdf_file.name}')

                        out_path = f'{pdf_file.name}.csv'
                        with open(out_path) as f:
                            st.download_button(pdf_file.name, file_name=f'{pdf_file.name}.csv', data=f)

if __name__ == '__main__':
    main()