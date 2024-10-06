--
-- PostgreSQL database dump
--

-- Dumped from database version 16.1
-- Dumped by pg_dump version 16.1

SET statement_timeout = 0;

SET lock_timeout = 0;

SET idle_in_transaction_session_timeout = 0;

SET client_encoding = 'UTF8';

SET standard_conforming_strings = on;

SELECT pg_catalog.set_config('search_path', '', false);

SET check_function_bodies = false;

SET xmloption = content;

SET client_min_messages = warning;

SET row_security = off;


SET default_tablespace = '';


SET default_table_access_method = heap;


--
-- Name: funcoes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.funcoes (
    id bigint NOT NULL,
    name character varying(255),
    query text
);



ALTER TABLE public.funcoes OWNER TO postgres;


--
-- Name: funcoes_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.funcoes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE public.funcoes_id_seq OWNER TO postgres;


--
-- Name: funcoes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.funcoes_id_seq OWNED BY public.funcoes.id;



--
-- Name: funcoes id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.funcoes ALTER COLUMN id SET DEFAULT nextval('public.funcoes_id_seq'::regclass);
COPY public.funcoes FROM stdin;
1	teste query	INSERT INTO cars (brand, model, year) VALUES ('Fo ESDF SDF SDFSDF SDF\nSDFSDF\nSDFSDF\nrd', 'Mustang', 1964);
\.
COPY public.funcoes FROM stdin;
2	outro teste	sdfsdfs sdf sdf
\.



--
-- Name: funcoes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.funcoes_id_seq', 2, true);



--
-- Name: funcoes funcoes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.funcoes
    ADD CONSTRAINT funcoes_pkey PRIMARY KEY (id);



--
-- PostgreSQL database dump complete
--

