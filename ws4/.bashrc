#Kell's personal .bashrc additions

#echo "sourcing .bashrc.mine"
#I usually keep my own stuff in .bashrc.mine so I don't mix it up with the
#built-in .bashrc, to which I just add 
#if [ -f ~/.bashrc.mine ]; then
#    . ~/.bashrc.mine
#fi

alias vi='vim'
alias bd='cd $OLDPWD'
alias cs='cd ~/cs131'

cdl () {
	if [ -n "$1" ]; then       #if $1 str length nonzero
		#echo "you supplied a nonzero string"
		cd "$1" && ls
	else
		#echo "cding home"
		cd ~ && ls
	fi
}
