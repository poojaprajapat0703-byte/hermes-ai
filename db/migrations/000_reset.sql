-- Clean slate: drop all tables and recreate from scratch
DROP TABLE IF EXISTS human_feedback CASCADE;
DROP TABLE IF EXISTS eval_runs CASCADE;
DROP TABLE IF EXISTS rca_reports CASCADE;
DROP TABLE IF EXISTS analyses CASCADE;
DROP TABLE IF EXISTS incidents CASCADE;
