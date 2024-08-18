
![alt text](https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/ab15e7be-ab9a-4fe2-966e-63d73c3437be/d9sqoap-c6e1e41d-d2b5-4d92-92c1-27c92e072bfd.jpg?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1cm46YXBwOjdlMGQxODg5ODIyNjQzNzNhNWYwZDQxNWVhMGQyNmUwIiwiaXNzIjoidXJuOmFwcDo3ZTBkMTg4OTgyMjY0MzczYTVmMGQ0MTVlYTBkMjZlMCIsIm9iaiI6W1t7InBhdGgiOiJcL2ZcL2FiMTVlN2JlLWFiOWEtNGZlMi05NjZlLTYzZDczYzM0MzdiZVwvZDlzcW9hcC1jNmUxZTQxZC1kMmI1LTRkOTItOTJjMS0yN2M5MmUwNzJiZmQuanBnIn1dXSwiYXVkIjpbInVybjpzZXJ2aWNlOmZpbGUuZG93bmxvYWQiXX0.L8DD-1wES0TG3BL1hL72ADFhahth_278Xa7s7hSjNI8)

# Projeto1



Bem-vindo ao 🚀 Projeto1! Este projeto utiliza dados históricos de Fórmula 1 obtidos da [API Ergast](http://ergast.com/mrd/db/#csv).

## Instalação e Configuração

### Clonando o Repositório

Para começar, clone este repositório para o seu ambiente local:

```bash
git clone https://github.com/danielcs7/cientistaDados.git
cd cientistaDados/projeto1
```

## 🔧 Instalando o Pyenv

Certifique-se de ter o pyenv instalado para gerenciar a versão do Python. Se ainda não o tiver, você pode instalá-lo seguindo as instruções [Pyenv](https://github.com/pyenv/pyenv)

## Configurando a Versão do Python

Utilize o pyenv para instalar e definir a versão do Python para 3.11.5:
```bash
pyenv install 3.11.5
pyenv global 3.11.5
```

## Criando o Ambiente Virtual

Crie um ambiente virtual com a versão do Python especificada:
```bash
python -m venv venv
```

## Ativando o Ambiente Virtual
### Ative o ambiente virtual:

* No macOS/Linux:
```bash
source venv/bin/activate
```
* No Windows:
```bash
venv\Scripts\activate
```

## 🛠️ Construído com

<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" /> 


## Instalando as Dependências

Instale as dependências do projeto. As dependências estão listadas no arquivo <b>requirements.txt</b>. Caso não exista um <b>requirements.txt</b>, você pode criar um com as dependências atuais do ambiente:

```bash
pip install -r requirements.txt
```
Se você precisar gerar um requirements.txt a partir das dependências instaladas, execute:

```bash
pip freeze > requirements.txt
```

## Desativando o Ambiente Virtual
Para desativar o ambiente virtual, basta executar:

```bash
deactivate
```

## Fontes dos Dados
Os dados utilizados neste projeto foram obtidos da API Ergast.
```perl
Você pode copiar e colar esse conteúdo diretamente no seu arquivo `README.md`. Se precisar de mais alguma coisa, é só avisar!
```