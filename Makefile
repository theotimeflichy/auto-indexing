PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)

MODULES = auto_indexing
EXTENSION = auto_indexing
DATA = auto_indexing--1.0.sql

# Compilation du module
all: $(MODULES).so

include $(PGXS)

# Nettoyage des fichiers générés
clean:
	rm -f $(MODULES).so auto_indexing.o
