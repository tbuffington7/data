library('RPostgreSQL')

drv <- dbDriver("PostgreSQL")


conn <- dbConnect(drv, dbname=Sys.getenv("NFIRS_DATABASE_NAME"),
                       user=Sys.getenv("NFIRS_DATABASE_USER"),
                       port=Sys.getenv("NFIRS_DATABASE_PORT"),
                       host=Sys.getenv("NFIRS_DATABASE_HOST"),
                       password=Sys.getenv("NFIRS_DATABASE_PASSWORD"))

fires <- dbGetQuery( conn, 'select *, fdid as fd_id from nist.final_query' )
set.seed( 953016876 )
# set all the fire results to zero where the query above returns a Null value.
for( i in c( 'res_all', 'res_2','res_3' ) ) fires[[ i ]][ is.na( fires[[ i ]] ) ] <- 0



fires$region[ fires$state == 'PR' ] <- 'Puerto Rico'
fires$region[ is.na( fires$region ) ] <- 'West'
fires$inc_hh <- as.numeric( fires$inc_hh )
fires$inc_hh <- log( fires$inc_hh )
for( i in c( 'region', 'state', 'fd_id', 'fd_size' ) ) fires[[ i ]] <- factor( fires[[ i ]] )
fires$region <- relevel( fires$region, 'West' )

# create filters
fires$no.fire <- fires$giants <- fires$small <- fires$base <- fires$include <- TRUE

# base filter
fires$base <- with( fires, fd_size %in% 3:9 & ! ( is.na( fd_id ) | is.na( fd_size ) ) )
fires$base <- fires$base & ! is.na( fires$inc_hh )
fires$base <- fires$base & fires$f_located > 0
fires$base <- fires$base & ! is.na( fires$smoke_cty )

# giants filter
# changed geoid to tr10_fid
u <- with( fires[ fires$base, ], list( pop=tr10_fid[ pop > quantile( pop, .999) ],
                                         hse.units=tr10_fid[hse_units > quantile(hse_units, .999)],
                                         males=tr10_fid[males > quantile(males, .999)],
                                         age_45_54=tr10_fid[age_45_54 > quantile(age_45_54, .999)]))
# changed geoid to tr10_fid
v <- NULL
for( i in names( u ) ) v <- union( v, u[[ i ]] )
fires$giants <- ! fires$tr10_fid %in% v
rm( i, u, v )

# small filter
fires$small <- fires$dept_incidents > 25 & ! is.na( fires$dept_incidents )

fires$include <- with( fires, base & small & giants )


# define outliers
dept <- fires[, c( 'tr10_fid', 'year', 'fd_id', 'dept_incidents' ) ]
ddd <- unique( fires[,c( 'year', 'fd_id', 'dept_incidents' ) ] )
ddd <- aggregate( ddd$dept_incidents, list( fd_id=ddd$fd_id ), function( x ) c( mean( x, na.rm=TRUE ), sd( x, na.rm=TRUE ) ))
ddd$m <- ddd$x[,1]
ddd$sd <- ddd$x[,2]
dept$m <- ddd$m[ match( dept$fd_id, ddd$fd_id ) ]
dept$sd <- ddd$sd[ match( dept$fd_id, ddd$fd_id ) ]
dept$lg <- ! ( is.na( dept$dept_incidents ) | dept$dept_incidents < dept$m - 2 * dept$sd )
fires$lcl <- dept$lg
rm( dept, ddd )

# partition data
tr10_fid <- unique( fires$tr10_fid )
tr10_fid <- data.frame( tr10_fid=tr10_fid, v=floor( runif( length( tr10_fid ) ) * 3 ), set="",
                       stringsAsFactors=FALSE )
tr10_fid$set[ tr10_fid$v == 0 ] <- "training"
tr10_fid$set[ tr10_fid$v == 1 ] <- "validation"
tr10_fid$set[ tr10_fid$v == 2 ] <- "test"
tr10_fid$set <- factor( tr10_fid$set )
fires$set <- tr10_fid$set[ match( fires$tr10_fid, tr10_fid$tr10_fid ) ]
rm( tr10_fid )


f <- function( conn, group=NULL, y=NULL, mdls=NULL, run="short" )
{
        if( is.null( group ) & is.null( y ) ) stop( "at least one of 'group' and 'y' must be specified" )
        if( ! ( is.null( group ) | class( group ) == "character" ) ) stop( "If you use 'group' it must be a character vector" )

        if( is.null( group ) & ( is.null( y ) | is.null( mdls ) ) ) stop( "both 'y' and 'mdls' must be specified" )
        if( is.null( group ) & ! ( is.character( y ) & is.character( mdls ) ) ) stop( "both 'y' and 'mdls' must be character vectors" )

        r0 <- dbGetQuery( conn, paste( "select * from controls.runs where grp = '", run, "' order by tier1, tier2", sep="" ) )
        r <- list()
        tier1 <- unique( r0$tier1 )

        for( i in tier1 )
        {
                r1 <- subset( r0, tier1 == i )
                if( is.na( r1$tier2[ 1 ] ) ) r[[ i ]] <- parse( text=r1$value[ 1 ] )[[ 1 ]]
                else
                {
                        r[[ i ]] <- list()
                        for( j in 1:nrow( r1 ) ) r[[ i ]][[ r1$tier2[ j ] ]] <- parse( text=r1$value[ j ] )[[ 1 ]]
                }
        }

        if( ! is.null( group ) )
        {
                mdl <- dbGetQuery( conn, paste( "select * from controls.models where lst in ( '", paste( group, collapse="', '" ), "' )", sep="" ) )
                npts <- dbGetQuery( conn, paste( "select * from controls.inputs where lst in ( '", paste( group, collapse="', '" ), "' )", sep="" ) )
        }
        else
        {
                mdl <- dbGetQuery( conn, paste( "select * from controls.models where target='", y, "' AND model in ( '", paste( mdls, collapse="', '" ), "' )", sep="" ) )
                npts <- dbGetQuery( conn, paste( "select * from controls.models NATURAL JOIN controls.inputs where target='", y, "' AND model in ( '", paste( mdls, collapse="', '" ), "' )", sep="" ) )
        }

        models <- NULL
        for( i in 1:nrow( mdl ) )
        {
                models[[ mdl$model[ i ] ]] <- list( fn=c( library=mdl$library[ i ], ff=mdl$ff[ i ] ), inputs=list() )
                npt0 <- subset( npts, lst==mdl$lst[ i ] & model==mdl$model[ i ] )
                for( j in 1:nrow( npt0 ) )
                {
                        if( npt0$class[ j ] == "call" )
                                models[[ i ]]$inputs[[ npt0$input[ j ] ]] <- parse( text=npt0$value[ j ] )[[ 1 ]]
                        else if( npt0$class[ j ] == "formula" )
                                models[[ i ]]$inputs[[ npt0$input[ j ] ]] <- as.formula( npt0$value[ j ], env=.GlobalEnv )
                        else
                                models[[ i ]]$inputs[[ npt0$input[ j ] ]] <- do.call( paste( "as", npt0$class[ j ], sep="." ), list( x=npt0$value[ j ] ) )
                }
        }

        list( models=models, runs=r )

}



### Run function

fn.run <- function( sets, n=0, sink=NULL )
{
    require( boot )
    require( utils )

#   u <- Sys.time()
    out <<- list()
    if( ! is.null( sink ) )
    {
        if( is.character( sink ) )      ff <- file( sink, "w" )
        else
        {
            warning( "the 'sink' term must be a character" )
            ff <- NULL
        }
    }
    else ff <- NULL

    for( k in names( sets$models ) )
    {
                if( tolower( sets$models[[k]]$fn[ 'library' ] ) == "null" ) next
        out[[k]] <<- list()

                require( sets$models[[k]]$fn[ 'library' ], character.only=TRUE )
                fn  <- sets$models[[k]]$fn[ 'ff' ]

        aa <- a <- sets$models[[k]]$inputs
        subset.a <- a$subset
        a$subset <- NULL
        data <- a$data

        for( i in names( sets$runs ) )
        {
            out[[k]][[i]] <<- list()
            if( is.list( sets$runs[[i]] ) )
            {
                for( j in names( sets$runs[[i]] ) )
                {
#                   u[1] <- Sys.time()
                    out[[k]][[i]][[j]] <<- list()
#                   cat( "Evaluating ", k, format( " model: ", width=16 - nchar( k ) ), i, " ", j, format( ":", width=11 - nchar( j ) ), sep="" )
                    aa$subset <- substitute( u & v & set %in% c( "training", "validation" ), list( u=sets$runs[[i]][[j]], v=subset.a ) )

                    if( ! is.null( ff ) ) sink( ff, type="message", append=TRUE )
                    tryCatch(
                        out[[k]][[i]][[j]]$model <<- do.call( fn, aa ),
                        error  =function( e ) cat(   "ERROR in Model: ", k, ", run ", i, "-", j, ". Message: ", e$message, "\n", sep="", file=stderr() ),
                        message=function( e ) cat( "MESSAGE in Model: ", k, ", run ", i, "-", j, ". Message: ", e$message, "\n", sep="", file=stderr() )
                    )
                    if( ! is.null( ff ) ) sink( type="message" )
                    if( n > 0 )
                    {
                        dta <- do.call( "subset", list( x=data, subset=aa$subset ) )
#                                               pb <- winProgressBar( title=paste( "Bootstrapping ", k, " model: ", i, " ", j, ": ", n, " iterations", sep="" ), label="0", max=n )
                                                        out[[k]][[i]][[j]]$boot <<- boot( dta, bbb, R=n, strata=dta$fd_id, a=a, ff=ff, fn=fn, pb=pb, nme=names( fixef( out[[k]][[i]][[j]]$model ) ) )
#                                               close( pb )
                    }

#                   u[2] <- Sys.time()
#                   cat( "Elapsed time:", format( u[2] - u[1] ), "\n" )
                }
            }
            else
            {
#               u[1] <- Sys.time()
#               cat( "Evaluating ", k, format( " model: ", width=16 - nchar( k ) ), i, " all models:", sep="" )
                aa$subset <- substitute( u & v & set %in% c( "training", "validation" ), list( u=sets$runs[[i]], v=subset.a ) )

                if( ! is.null( ff ) ) sink( ff, type="message", append=TRUE )
                tryCatch(
                    out[[k]][[i]]$model <<- do.call( fn, aa ),
                    error  =function(e) cat(  "ERROR in Model: ",k,", run ",i,"-All. Message: ",e$message,"\n",sep="",file=stderr()),
                    message=function(e) cat("MESSAGE in Model: ",k,", run ",i,"-All. Message: ",e$message,"\n",sep="",file=stderr() )
                )
                if( ! is.null( ff ) ) sink( type="message" )
                if( n > 0 )
                {
                    dta <- do.call( "subset", list( x=data, subset=aa$subset ) )
#                                       pb <- winProgressBar( title=paste( "Bootstrapping ", k, " model: ", i, " all models: ", n, " iterations", sep="" ), label="0", max=n )
                                                out[[k]][[i]]$boot <<- boot( dta, bbb, R=n, strata=dta$fd_id, a=a, ff=ff, fn=fn, pb=pb, nme=names( fixef( out[[k]][[i]]$model ) ) )
#                                       close( pb )
                }

#               u[2] <- Sys.time()
#               cat( "Elapsed time:", format( u[2] - u[1] ), "\n" )
            }
        }
    }
    if( ! is.null( ff ) ) close( ff )
}


### test function

fn.test <- function( input, output, subset=NULL )
{
#   Test to see if the data and dependent variables are all identical.
#   If not, throw an error
    x <- unlist( lapply( input$models, function( x ) as.character( x$inputs$data   ) ) )
    dta <- x[1]
    if( ! all( dta == x ) ) stop( "data are not all identical. Try breaking up the input and output files." )
    x <- unlist( lapply( input$models, function( x ) as.character( x$inputs$formula[ 2 ] ) ) )
    y <- x[1]
    if( ! all( y == x ) ) stop( "Dependent variables are not all identical. Try breaking up the input and output files." )
    rm( x )

    if( is.null( subset ) )
    {
        x <- unlist( lapply( input$models, function( x ) as.character( x$inputs$subset ) ) )
        if( ! all( x[1] == x ) ) warning( "The subsets are not all identical. Using the first. Try specifying the subset you want.")
        rm( x )
        subset <- input$models[[ 1 ]]$inputs$subset
    }

    if( is.list( subset ) )
    {
        old.res <- subset
        subset <- old.res$subset
        results <- old.res$results
        if( y != old.res$lhs ) stop( "When 'subset' is the old results list, then the dependent variables must match." )
    }

    new.data <- do.call( "subset", list( x=get( dta ), subset=substitute( a & set == "test", list( a=subset ) ) ) )

    if( ! exists( "old.res" ) )
    {
        results <- new.data[ , c( "year", "tr10_fid", "state", "region", "fd_id", "fd_size" ) ]
        results$dept.new <- as.character( NA )
        if( deparse( input$models[[1]]$inputs$family ) == "binomial" )
        {
            tmp.y <- eval( parse( text=y ), envir=new.data )
            results[[ y ]] <- tmp.y[,1] / ( tmp.y[,1] + tmp.y[,2] )
        }
        else results[[ y ]] <- eval( parse( text=y ), envir=new.data )
    }

    vars <- NULL
    for( k in names( input$models ) )
    {
        if( tolower( input$models[[k]]$fn["library"] ) == "null" ) next
        vars <- c( vars, k )
        require( input$models[[k]]$fn["library"], character.only=TRUE )
        results[[ k ]] <- as.numeric( NA )

    for( i in names( input$runs ) )
    {
        if( is.list( input$runs[[i]] ) )
        {
            for( j in names( input$runs[[i]] ) )
            {
                x <- eval( input$runs[[i]][[j]], envir=new.data )
                if( any( x ) )
                {
                    if( is.null( output[[k]][[i]][[j]]$model ) )
                    {
                        z <- as.numeric( NA )
                        warning( paste( "WARNING: Model ", k, " run ", i, "-", j, ": No model results were found.", sep="" ) )
                    }
                    else tryCatch(
                        {
                            if( input$models[[k]]$fn["library"] == "lme4" )
                            {
                                z <- predict( output[[k]][[i]][[j]]$model,newdata=new.data[x,],type="response",allow.new.levels=TRUE )
                                x1 <- results$fd_id %in% row.names( ranef( output[[k]][[i]][[j]]$model )$fd_id )
                                results$dept.new[ x & ! x1 ] <- paste( results$dept.new[ x & ! x1 ], k, sep=";" )
                            }
                        else if( input$models[[k]]$fn["library"] == "glmnet" )
                            {
                                d.f <- model.frame( formula=input$models[[k]]$inputs$formula, data=new.data[x,], na.action=na.pass )
                                off  <- eval( input$models[[k]]$inputs$offset, new.data[x,] )
                                z <- predict( output[[k]][[i]][[j]]$model, newx=model.matrix( terms( d.f ), d.f ), offset=off )
                            }
                            else
                            {
                                z <- predict( output[[k]][[i]][[j]]$model, newdata=new.data[x,], type="response" )
                            }
                        },
                        error=function( e ) stop( paste( "ERROR: Model ", k, " run ", i, "-All: ", e$message, sep="" ) )
                    )
                    results[[ k ]][x] <- z
                }
            }
        }
        else
        {
            x <- eval( input$runs[[i]], envir=new.data )
            if( any( x ) )
            {
                if( is.null( output[[k]][[i]]$model ) )
                {
                    z <- as.numeric( NA )
                    warning( paste( "WARNING: Model ", k, " run ", i, "-All: No model results were found.", sep="" ) )
                }
                else tryCatch(
                    {
                        if( input$models[[k]]$fn["library"] == "lme4" )
                        {
                            z <- predict( output[[k]][[i]]$model, newdata=new.data[x,], type="response", allow.new.levels=TRUE )
                            x1 <- results$fd_id %in% row.names( ranef( output[[k]][[i]]$model )$fd_id )
                            results$dept.new[ x & ! x1 ] <- paste( results$dept.new[ x & ! x1 ], k, sep=";" )
                        }
                        else if( input$models[[k]]$fn["library"] == "glmnet" )
                        {
                            d.f <- model.frame( formula=input$models[[k]]$inputs$formula, data=new.data[x,], na.action=na.pass )
                            off  <- eval( input$models[[k]]$inputs$offset, new.data[x,] )
                            z <- predict( output[[k]][[i]]$model, newx=model.matrix( terms( d.f ), d.f ), offset=off )
                        }
                        else
                        {
                            z <- predict( output[[k]][[i]]$model, newdata=new.data[x,], type="response" )
                        }
                    },
                    error=function( e ) stop( paste( "ERROR: Model ", k, " run ", i, "-All: ", e$message, sep="" ) )
                )
                results[[ k ]][x] <- z
            }
        }
        }
    }
    results$dept.new <- sub( "^NA;", "", results$dept.new )
#   results$dept.new[ is.na( results$dept.new ) ] <- ""

    s <- results[ , vars ]
    s <- ( s - results[[ y ]] ) ^ 2
    se <- sqrt( colSums( s, na.rm=TRUE ) / apply( s, 2, function( x ) length( x[ ! is.na( x ) ] ) ) )
    if( exists( "old.res" ) ) list( lhs=y, subset=subset, se=c( old.res$se, se ), results=results )
        else list( lhs=y, subset=subset, se=se, results=results )

}


naive <- function( test )
{
        if( ! is.list( test ) ) stop( "this is not the output of the fn.test function" )
        if( any( names( test ) != c( "lhs", "subset", "se", "results" ) ) ) stop( "this is not the output of the fn.test function" )

        x <- test$results[, c( "year", "geoid", test$lhs ) ]
        x$ndx <- paste( x$geoid, x$year, sep="." )
        x$match <- paste( x$geoid, x$year - 1, sep="." )

        x$naive <- x[[ test$lhs ]][ match( x$match, x$ndx ) ]
        if( "f_located" %in% names( test$results ) )
        {
                x$f_located <- test$results$f_located
                x$naive <- x$naive * x$f_located / x$f_located[ match( x$match, x$ndx ) ]
        }
        test$results$naive <- x$naive
        s <- ( test$results[[ test$lhs ]] - test$results$naive ) ^ 2
        test$se <- c(  test$se, naive=sqrt( sum( s, na.rm=TRUE ) / length( s[ ! is.na( s ) ] ) ))
        test
}

inputs <- f(conn, 'npt.sz2.L0a', 'sz2')
fn.run(inputs)
