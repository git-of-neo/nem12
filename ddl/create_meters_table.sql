create table meter_readings (
    id uuid default gen_random_uuid() not null,
    "nmi" varchar(10) not null,
    "timestamp" timestamp not null,
    "consumption" numeric not null,
    constraint meter_readings_pk primary key (id),
    constraint meter_readings_unique_consumption unique ("nmi", "timestamp")
);
