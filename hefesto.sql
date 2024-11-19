CREATE TABLE Curso (
    id_curso INT PRIMARY KEY,
    nome_curso VARCHAR(255)
);
CREATE TABLE Aluno (
    RA INT PRIMARY KEY,
    nome_aluno VARCHAR(255),
    ano_matricula INT,
    id_curso INT,
	codigo_disc INT
);

CREATE TABLE Disciplina (
    codigo_disc INT PRIMARY KEY,
    nome_disc VARCHAR(255),
	ano_disc INT,
	semestre_disc INT,
	codigo_prof INT,
	id_curso INT
);

CREATE TABLE MatrizCurricular (
	id_matriz INT PRIMARY KEY,
   	semestre_aprovado INT,
	ano_aprovado INT,
	id_curso INT,
	codigo_disc INT
);

CREATE TABLE HistEsc (
    nota NUMERIC,
    semestre_cursado INT,
	ano_cursado INT, 
	RA INT PRIMARY KEY,
	codigo_disc INT
	
);

CREATE TABLE HistDisc(
	ano_ministrado INT,
	semestre_ministrado INT,
	codigo_prof INT,
	codigo_disc INT PRIMARY KEY
	
);
CREATE TABLE Departamento(
	nome_dep VARCHAR(255) PRIMARY KEY,
	codigo_dep VARCHAR(255),
	nome_prof VARCHAR(255),
	codigo_disc INT, 
	id_curso INT
);

CREATE TABLE Professor(
	codigo_prof INT PRIMARY KEY,
	nome_prof VARCHAR(255),
	chefe_dep BOOLEAN,
	id_curso INT
);

CREATE TABLE Professor_Departamento (
    codigo_prof INT,
    nome_dep VARCHAR(255),
    FOREIGN KEY (codigo_prof) REFERENCES Professor(codigo_prof),
    FOREIGN KEY (nome_dep) REFERENCES Departamento(nome_dep),
    PRIMARY KEY (codigo_prof, nome_dep)
);

CREATE TABLE TCC(
	id_TCC INT PRIMARY KEY,
	RA INT
);

CREATE TABLE Formados(
	RA INT,
	id_matriz INT,
	FOREIGN KEY (RA) REFERENCES Aluno(RA),
    FOREIGN KEY (id_matriz) REFERENCES MatrizCurricular(id_matriz),
	PRIMARY KEY (RA, id_matriz)
);

ALTER TABLE Aluno ADD id_TCC INT;

ALTER TABLE Aluno ADD CONSTRAINT fk_aluno_tcc FOREIGN KEY (id_TCC) REFERENCES TCC(id_TCC);

ALTER TABLE Professor ADD id_TCC INT;

ALTER TABLE Professor ADD CONSTRAINT fk_professor_tcc FOREIGN KEY (id_TCC) REFERENCES TCC(id_TCC)