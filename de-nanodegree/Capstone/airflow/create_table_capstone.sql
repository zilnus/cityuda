CREATE TABLE public.immigrant (
    cicid int4,
	i94yr int4,
	i94mon int4,
	i94cit int4,
	i94res int4,
	i94port varchar(256),
	arrdate float,
	i94mode int4,
	i94addr varchar(256),
	depdate float,
	i94bir int4,
	i94visa int4,
	biryear int4,
	gender varchar(256),
	airline varchar(256),
	fltno varchar(256),
	visatype varchar(256),
	arrival_date date,
	departure_date date,
	duration_stay int4,
	CONSTRAINT immig_pkey PRIMARY KEY (cicid)
);

CREATE TABLE public.country (
    country_code int4,
	country_name varchar(256),
	avg_temperature float,
	latitude varchar(256),
	longitude varchar(256),
	CONSTRAINT country_pkey PRIMARY KEY (country_code)
);

CREATE TABLE public.port (
    port_code varchar(256),
	port_name varchar(256),
	CONSTRAINT port_pkey PRIMARY KEY (port_code)
);

CREATE TABLE public.mode_transport (
    mode_code int4,
	mode_name varchar(256),
	CONSTRAINT modetrans_pkey PRIMARY KEY (mode_code)
);

CREATE TABLE public.visa_category (
    visacat_code int4,
	visa_category varchar(256),
	CONSTRAINT visacat_pkey PRIMARY KEY (visacat_code)
);

CREATE TABLE public.calendar (
    date_deparr date,
	year_deparr int4,
	month_deparr int4,
	day_deparr int4,
	quarter_deparr int4
	CONSTRAINT calendar_pkey PRIMARY KEY (date_deparr)
);

CREATE TABLE public.state (
    state_code varchar(256),
	state_name varchar(256),
	avg_medianage float,
	total_male bigint,
	total_female bigint,
	total_population bigint,
	total_veteran bigint,
	total_foreignborn bigint,
	total_americannative bigint,
	total_asian bigint,
	total_african bigint,
	total_hispanic bigint,
	total_white bigint,
	min_avghousesize float,
	max_avghousesize float,
	CONSTRAINT state_pkey PRIMARY KEY (state_code)
);





