all:
	xelatex -file-line-error main.tex
	nohup xdg-open main.pdf &
clean:
	@rm -rf *.log *.aux *.toc *.out *.synctex.gz *.fls *.fdb_latexmk
